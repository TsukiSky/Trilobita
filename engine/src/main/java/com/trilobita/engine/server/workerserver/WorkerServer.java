package com.trilobita.engine.server.workerserver;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trilobita.commons.Mail;
import com.trilobita.commons.MailType;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.core.messaging.MessageConsumer;
import com.trilobita.core.messaging.MessageProducer;
import com.trilobita.engine.server.AbstractServer;
import com.trilobita.engine.server.common.ServerStatus;
import com.trilobita.engine.server.workerserver.execution.ExecutionManager;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;
import java.util.concurrent.*;

/**
 * Worker Server controls vertex computations and communicate with other servers
 */
@Slf4j
public class WorkerServer extends AbstractServer {
    private final ExecutionManager executionManager;
    private final ConcurrentHashMap<Integer, CopyOnWriteArrayList<Mail>> outMailTable;
    private final MessageConsumer partitionMessageConsumer;
    private final MessageConsumer startMessageConsumer;

    public WorkerServer(int serverId, int numOfExecutor) throws ExecutionException, InterruptedException {
        super(serverId);
        this.executionManager = new ExecutionManager(4, this);
        this.outMailTable = new ConcurrentHashMap<>();
        this.setServerStatus(ServerStatus.START);
        this.partitionMessageConsumer= new MessageConsumer(this.getServerId() + "partition", serverId, new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail value, int partition, long offset) throws JsonProcessingException, InterruptedException, ExecutionException {
                ObjectMapper objectMapper = new ObjectMapper();
                vertexGroup = objectMapper.convertValue(value.getMessage().getContent(), VertexGroup.class);
                log.info("Vertex Group: "+vertexGroup);
                start();
            }
        });
        this.startMessageConsumer = new MessageConsumer("start", serverId, new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail value, int partition, long offset) throws JsonProcessingException, InterruptedException {
                log.info("start new super step...");
                execute();
            }
        });
        startMessageConsumer.start();
        partitionMessageConsumer.start();
    }

    @Override
    public void start() throws InterruptedException {
        execute();
    }

    private void execute() throws InterruptedException {
        log.info("entering new super step...");
        this.executionManager.execute();
        MessageProducer.produce(null, new Mail(-1,null,MailType.FINISH_INDICATOR), "finish");
    }

    @Override
    public void pause() {
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
        Vertex vertex = findVertexById(mail.getToVertexId());
        if (vertex != null) {
            vertex.onReceive(mail);
        }
    }

    private Vertex findVertexById(int vertexId) {
        return this.getVertexGroup().getVertexById(vertexId);
    }
}
