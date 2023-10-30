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

import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * Worker Server controls vertex computations and communicate with other servers
 */

@Slf4j
@Getter
public class WorkerServer<T> extends AbstractServer<T> {
    private final ExecutionManager<T> executionManager;
    private final ConcurrentHashMap<Integer, CopyOnWriteArrayList<Mail>> outMailTable;
    private final ConcurrentHashMap<Integer, Computable<T>> vertexValues;
    private final MessageConsumer partitionMessageConsumer;
    private final MessageConsumer startMessageConsumer;
    private final MessageConsumer faultHandleMessageConsumer;

    public WorkerServer(int serverId, int numOfExecutor) throws ExecutionException, InterruptedException {
        super(serverId);
        this.executionManager = new ExecutionManager<>(4, this);
        this.outMailTable = new ConcurrentHashMap<>();
        this.vertexValues = new ConcurrentHashMap<>();
        this.setServerStatus(ServerStatus.START);
        this.partitionMessageConsumer= new MessageConsumer(this.getServerId() + "partition", serverId, new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail value, int partition, long offset) throws JsonProcessingException, InterruptedException, ExecutionException {
                vertexGroup = (VertexGroup<T>) value.getMessage().getContent();
                //    assign the server's hashmap to each vertex
                List<Vertex<T>> vertices = vertexGroup.getVertices();
                for (Vertex<T> vertex: vertices){
                    vertex.setServerQueue(getOutMailQueue());
                    vertex.setVertexValues(vertexValues);
                }
                log.info("Vertex Group: "+vertexGroup);
                start();
            }
        });

        this.startMessageConsumer = new MessageConsumer("start", serverId, new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail value, int partition, long offset) throws JsonProcessingException, InterruptedException {
                log.info("start new super step...");
                if (getVertexGroup() != null){
                    execute();
                }
            }
        });

        this.faultHandleMessageConsumer= new MessageConsumer("fault-handle", serverId, new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail value, int partition, long offset) throws JsonProcessingException, InterruptedException {
                log.info("detected server down...");
                pause();
            }
        });

        startMessageConsumer.start();
        partitionMessageConsumer.start();
    }

    @Override
    public void start() throws InterruptedException {
        setServerStatus(ServerStatus.RUNNING);
        execute();
    }

    private synchronized void execute() throws InterruptedException {
        log.info("entering new super step...");
        this.executionManager.execute();
        // send the value of the current superstep to the master
        Message message = new Message(vertexValues, Message.MessageType.NORMAL);
        Mail mail = new Mail(-1, message, Mail.MailType.NORMAL);
        log.info("mail value: {}", mail);
        MessageProducer.produce(null, mail, "finish");
    }

    @Override
    public void pause() {
        setServerStatus(ServerStatus.PAUSE);
    }

    @Override
    public void shutdown() {
    }

    public void onStartSignal() throws InterruptedException {
        this.start();
    }

    public void sendCompleteSignal() {

    }

    public void distributeMailToVertex(Mail mail) {
        Vertex<T> vertex = findVertexById(mail.getToVertexId());
        if (vertex != null) {
            vertex.onReceive(mail);
        }
    }

    private Vertex<T> findVertexById(int vertexId) {
        return this.getVertexGroup().getVertexById(vertexId);
    }
}
