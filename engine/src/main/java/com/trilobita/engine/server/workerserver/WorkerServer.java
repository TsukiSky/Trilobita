package com.trilobita.engine.server.workerserver;

import com.trilobita.commons.*;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.core.messaging.MessageConsumer;
import com.trilobita.core.messaging.MessageProducer;
import com.trilobita.engine.server.AbstractServer;
import com.trilobita.engine.server.heartbeat.HeartbeatSender;
import com.trilobita.engine.server.masterserver.partitioner.PartitionStrategy;
import com.trilobita.engine.server.workerserver.execution.ExecutionManager;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;

/**
 * Worker Server is the worker of a server cluster.
 * It executes the superstep, controls vertex computations and communicate with other servers
 *
 * @param <T> the type of the vertex value
 */
@Slf4j
@Getter
public class WorkerServer<T> extends AbstractServer<T> {
    private final ExecutionManager<T> executionManager;
    private final ConcurrentHashMap<Integer, CopyOnWriteArrayList<Mail>> outMailTable;
    private final MessageConsumer partitionMessageConsumer;
    private final MessageConsumer startMessageConsumer;
    private final HeartbeatSender heartbeatSender;

    public WorkerServer(int serverId, int parallelism, PartitionStrategy partitionStrategy) {
        super(serverId, partitionStrategy);
        this.executionManager = new ExecutionManager<>(parallelism, this);
        this.outMailTable = new ConcurrentHashMap<>();
        this.partitionMessageConsumer = new MessageConsumer("SERVER_" + this.getServerId() + "_PARTITION", serverId,
                new MessageConsumer.MessageHandler() {
                    @Override
                    public void handleMessage(UUID key, Mail mail, int partition, long offset) throws InterruptedException, ExecutionException {
                        log.info("receiving message from server.........");
                        WorkerServer.this.executionManager.waitForFutures(); // in case of fault, repartition is needed
                        Map<String, Object> res = (Map<String, Object>) mail.getMessage().getContent();
                        setVertexGroup((VertexGroup<T>) res.get("PARTITION"));
                        PartitionStrategy partitionStrategy = (PartitionStrategy) res.get("PARTITION_STRATEGY");
                        setPartitionStrategy(partitionStrategy);
                        // assign the server's hashmap to each vertex
                        List<Vertex<T>> vertices = vertexGroup.getVertices();
                        for (Vertex<T> vertex : vertices) {
                            vertex.setServerQueue(getOutMailQueue());
                        }
                        log.info("[Partition] Vertex Group: {}", vertexGroup);
                    }
                });

        this.startMessageConsumer = new MessageConsumer(Mail.MailType.START_SIGNAL.ordinal(), serverId, new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail mail, int partition, long offset) throws InterruptedException {
                if (getVertexGroup() != null) {
                    boolean doSnapshot = (boolean) mail.getMessage().getContent();
                    superstep(doSnapshot);
                }
            }
        });
        this.heartbeatSender = new HeartbeatSender(this.getServerId(), true);
    }

    /**
     * Start running the server
     */
    @Override
    public void start() throws InterruptedException, ExecutionException {
        setServerStatus(ServerStatus.RUNNING);
        startMessageConsumer.start();
        partitionMessageConsumer.start();
        this.getMessageConsumer().start();
        heartbeatSender.start();
    }

    /**
     * Execute the superstep
     */
    private void superstep(boolean doSnapshot) throws InterruptedException {
        superstep ++;
        log.info("[Superstep] entering a new super step...");
        this.executionManager.execute();
        sendCompleteSignal(doSnapshot);
    }

    @Override
    public void pause() {
        setServerStatus(ServerStatus.PAUSE);
    }

    @Override
    public void shutdown() {
    }

    /**
     * Send a complete signal to the master server
     */
    public void sendCompleteSignal(boolean doSnapshot) {
        log.info("[Superstep] super step {} completed", superstep);
        if (doSnapshot) {
            MessageProducer.produceFinishSignal(this.vertexGroup.getVertexValues());
        } else {
            MessageProducer.produceFinishSignal(new HashMap<>());
        }
    }

    /**
     * Distribute mail to vertex
     * @param mail mail to be distributed
     */
    public void distributeMailToVertex(Mail mail) {
        Vertex<T> vertex = findVertexById(mail.getToVertexId());
        if (vertex != null) {
            vertex.onReceive(mail);
        }
    }

    /**
     * Find the vertex with the given id
     * @param vertexId vertex id
     * @return vertex with the given id
     */
    private Vertex<T> findVertexById(int vertexId) {
        return this.getVertexGroup().getVertexById(vertexId);
    }
}
