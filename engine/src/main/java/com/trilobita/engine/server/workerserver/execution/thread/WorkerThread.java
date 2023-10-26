package com.trilobita.engine.server.workerserver.execution.thread;

import com.trilobita.commons.Mail;
import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.core.messaging.MessageProducer;
import com.trilobita.engine.computing.task.MailingTask;
import com.trilobita.engine.server.workerserver.WorkerServer;
import com.trilobita.engine.server.workerserver.execution.ExecutionManager;

import java.util.concurrent.CountDownLatch;

public class WorkerThread<T> extends Thread {
    private final WorkerServer<T> server;
    private final ExecutionManager<T> executionManager;

    public WorkerThread(WorkerServer<T> server, ExecutionManager<T> executionManager) {
        this.server = server;
        this.executionManager = executionManager;
    }

    @Override
    public void run() {
        // 1. distribute mails to vertex
        while (!server.getInMailQueue().isEmpty()) {
            Mail mail = this.server.getInMailQueue().poll();
            if (mail != null) {
                server.distributeMailToVertex(mail);
                try {
                    this.executionManager.activeVertices.put(mail.getToVertexId());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        // 2. compute vertex
        while (!this.executionManager.activeVertices.isEmpty()) {
            Integer vertexId = this.executionManager.activeVertices.poll();
            Vertex<T> vertex = this.server.getVertexGroup().getVertexById(vertexId);
            if (vertex != null) {
                vertex.compute();
            }
        }

        // 3. post mails
        while (!this.server.getOutMailQueue().isEmpty()) {
            Mail mail = this.server.getOutMailQueue().poll();
            int receiverId = this.server.findServerByVertexId(mail.getToVertexId());
            MessageProducer.produce(null, mail, receiverId + "");
        }
    }
}
