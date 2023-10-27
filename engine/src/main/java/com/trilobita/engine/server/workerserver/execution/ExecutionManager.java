package com.trilobita.engine.server.workerserver.execution;

import com.trilobita.commons.Mail;
import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.core.messaging.MessageProducer;
import com.trilobita.engine.server.workerserver.WorkerServer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * The execution manager on a single worker machine
 */
public class ExecutionManager {
    public final WorkerServer server;
    public final ExecutorService executorService;
    public List<Vertex> vertices;

    public ExecutionManager(int parallelism, WorkerServer server) {
        this.server = server;
        this.executorService = Executors.newFixedThreadPool(parallelism);
    }


    public void execute() throws InterruptedException {
        this.vertices = this.server.getVertexGroup().getVertices();
        while (!server.getInMailQueue().isEmpty()) {
            Mail mail = (Mail) this.server.getInMailQueue().poll();
            if (mail != null) {
                this.executorService.submit(() -> {
                    server.distributeMailToVertex(mail);
                });
            }
        }
        for (Vertex vertex: vertices) {
            if (vertex.getStatus() == Vertex.VertexStatus.ACTIVE) {
                executorService.submit(vertex::compute);
            }
        }
        while (!this.server.getOutMailQueue().isEmpty()) {
            Mail mail = (Mail) this.server.getOutMailQueue().poll();
            executorService.submit(() -> {
                int receiverId = this.server.findServerByVertexId(mail.getToVertexId());
                MessageProducer.produce(null, mail, receiverId + "");
            });
        }
        executorService.shutdown();
        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
