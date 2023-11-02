package com.trilobita.engine.server.workerserver;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trilobita.commons.*;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.core.messaging.MessageConsumer;
import com.trilobita.core.messaging.MessageProducer;
import com.trilobita.engine.server.AbstractServer;
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

    public WorkerServer(int serverId, int parallelism) throws ExecutionException, InterruptedException {
        super(serverId);
        this.executionManager = new ExecutionManager<>(parallelism, this);
        this.outMailTable = new ConcurrentHashMap<>();
        this.vertexValues = new ConcurrentHashMap<>();

        this.partitionMessageConsumer = new MessageConsumer("SERVER_" + this.getServerId() + "_PARTITION", serverId,
                new MessageConsumer.MessageHandler() {
                    @Override
                    public void handleMessage(UUID key, Mail mail, int partition, long offset) throws JsonProcessingException, InterruptedException, ExecutionException {
                        Map<String, Object> res = (Map<String, Object>) mail.getMessage().getContent();
                        setVertexGroup((VertexGroup<T>) res.get("PARTITION"));
                        setVertexToServer((HashMap<Integer, Integer>) res.get("VERTEX-TO-SERVER"));
                        //    assign the server's hashmap to each vertex
                        List<Vertex<T>> vertices = vertexGroup.getVertices();
                        for (Vertex<T> vertex : vertices) {
                            vertex.setServerQueue(getOutMailQueue());
                        }
                        superstep = 1;
                        log.info("Vertex Group: {}", vertexGroup);
                        log.info("Vertex to Server: {}", getVertexToServer());
                        start();
                    }
                });

        this.startMessageConsumer = new MessageConsumer(Mail.MailType.START_SIGNAL.ordinal(), serverId, new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail mail, int partition, long offset) throws InterruptedException {
                log.info("start new super step...");
                if (getVertexGroup() != null) {
                    execute();
                }
            }
        });

        startMessageConsumer.start();
        partitionMessageConsumer.start();
        this.getMessageConsumer().start();
    }

    @Override
    public void start() throws InterruptedException {
        setServerStatus(ServerStatus.RUNNING);
        execute();
    }

    private synchronized void execute() throws InterruptedException {
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
        MessageProducer.produceFinishSignal(this.superstep, new HashMap<>(vertexValues));
        superstep++;
    }

    /**
     * Distribute mail to vertex
     *
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
     *
     * @param vertexId vertex id
     * @return vertex with the given id
     */
    private Vertex<T> findVertexById(int vertexId) {
        return this.getVertexGroup().getVertexById(vertexId);
    }
}
