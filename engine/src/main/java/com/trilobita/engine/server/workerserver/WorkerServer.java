package com.trilobita.engine.server.workerserver;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trilobita.commons.*;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.core.messaging.MessageConsumer;
import com.trilobita.core.messaging.MessageProducer;
import com.trilobita.engine.server.AbstractServer;
import com.trilobita.engine.server.heartbeat.HeartbeatSender;
import com.trilobita.engine.server.masterserver.partitioner.PartitionStrategy;
import com.trilobita.engine.server.masterserver.partitioner.PartitionStrategyFactory;
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
    private final ConcurrentHashMap<Integer, Computable<T>> vertexValues;
    private final MessageConsumer partitionMessageConsumer;
    private final MessageConsumer startMessageConsumer;
    private final HeartbeatSender heartbeatSender;

    public WorkerServer(int serverId, int parallelism, PartitionStrategy partitionStrategy) throws ExecutionException, InterruptedException {
        super(serverId, partitionStrategy);
        this.executionManager = new ExecutionManager<>(parallelism, this);
        this.outMailTable = new ConcurrentHashMap<>();
        this.vertexValues = new ConcurrentHashMap<>();
        this.partitionMessageConsumer = new MessageConsumer("SERVER_" + this.getServerId() + "_PARTITION", serverId,
                new MessageConsumer.MessageHandler() {
                    @Override
                    public void handleMessage(UUID key, Mail mail, int partition, long offset) throws InterruptedException, ExecutionException {
                        log.info("receiving message from server.........");
                        WorkerServer.this.executionManager.waitForFutures(); // in case of fault, repartition is needed
                        Map<String, Object> res = (Map<String, Object>) mail.getMessage().getContent();
                        setVertexGroup((VertexGroup<T>) res.get("PARTITION"));
                        PartitionStrategy partitionStrategy = (PartitionStrategy) res.get("PARTITIONSTRATEGY");
                        log.info(partitionStrategy.getWorkerIdList().toString());
                        setPartitionStrategy(partitionStrategy);
                        // assign the server's hashmap to each vertex
                        List<Vertex<T>> vertices = vertexGroup.getVertices();
                        for (Vertex<T> vertex : vertices) {
                            vertex.setServerQueue(getOutMailQueue());
                            vertex.setServerVertexValue(getVertexValues());
                        }
                        superstep = 1;
                        log.info("Vertex Group: {}", vertexGroup);
                        startNewSuperstep();
                    }
                });

        this.startMessageConsumer = new MessageConsumer(Mail.MailType.START_SIGNAL.ordinal(), serverId, new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail mail, int partition, long offset) throws InterruptedException {
                if (getVertexGroup() != null) {
                    startNewSuperstep();
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
    private void startNewSuperstep() throws InterruptedException {
        log.info("entering a new super step...");
        this.executionManager.execute();
        sendCompleteSignal();
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
    public void sendCompleteSignal() {
        log.info("complete");
        MessageProducer.produceFinishSignal(this.superstep, new HashMap<>(vertexValues));
        superstep++;
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
