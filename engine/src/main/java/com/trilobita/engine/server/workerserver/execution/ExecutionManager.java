package com.trilobita.engine.server.workerserver.execution;

import com.trilobita.commons.*;
import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.core.messaging.MessageProducer;
import com.trilobita.engine.server.workerserver.WorkerServer;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.*;

/**
 * The execution manager on a single worker machine
 */
@Slf4j
public class ExecutionManager<T> {
    public final WorkerServer<T> server;
    public ExecutorService executorService;
    public List<Vertex<T>> vertices;
    public BlockingQueue<Integer> activeVertices;
    public CountDownLatch countDownLatch;
    public int parallelism;

    public ExecutionManager(int parallelism, WorkerServer<T> server) {
        this.server = server;
        this.parallelism = parallelism;
        this.executorService = Executors.newFixedThreadPool(parallelism);
    }

    public void execute() {
        this.countDownLatch = new CountDownLatch(this.parallelism);
        this.vertices = this.server.getVertexGroup().getVertices();
        if (executorService == null || executorService.isTerminated()) {
            executorService = Executors.newFixedThreadPool(5); // Adjust the number of threads as needed
        }
        while (!server.getInMailQueue().isEmpty()) {
            Mail mail = this.server.getInMailQueue().poll();
            if (mail != null) {
                this.executorService.submit(() -> {
                    server.distributeMailToVertex(mail);
                });
            }
        }
        for (Vertex<T> vertex: vertices) {
            if (vertex.getStatus() == Vertex.VertexStatus.ACTIVE) {
                executorService.submit(vertex::compute);
            }
        }
        while (!this.server.getOutMailQueue().isEmpty()) {
            Mail mail = this.server.getOutMailQueue().poll();
            executorService.submit(() -> {
                int receiverId = this.server.findServerByVertexId(mail.getToVertexId());
                MessageProducer.createAndProduce(null, mail, receiverId + "");
            });
        }

        // block until all threads finish
        executorService.shutdown();
        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
