package com.trilobita.engine.server.workerserver.execution;

import com.trilobita.commons.*;
import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.core.messaging.MessageProducer;
import com.trilobita.engine.server.workerserver.WorkerServer;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Execution Manager is responsible for the execution of a superstep
 *
 * @param <T> the type of the vertex value
 */
@Slf4j
public class ExecutionManager<T> {
    public final WorkerServer<T> server;
    private final ExecutorService executorService;
    private final List<Future<?>> futures = new ArrayList<>();

    public ExecutionManager(int parallelism, WorkerServer<T> server) {
        this.server = server;
        this.executorService = Executors.newFixedThreadPool(parallelism);
    }

    public void waitForFutures() throws ExecutionException, InterruptedException {
        for (Future<?> future : futures) {
            future.get(); // Wait for each task to complete
        }
    }

    /**
     * execute the superstep
     */
    public void execute() throws InterruptedException {
        futures.clear();
        // distribute the mail to the vertices
        while (!server.getInMailQueue().isEmpty()) {
            Mail mail = this.server.getInMailQueue().poll();
            if (mail != null) {
                futures.add(this.executorService.submit(() -> {
                    server.distributeMailToVertex(mail);
                }));
            }
        }

        log.info("[ExecutionManager] futures added server.distributeMailToVertex");

        // inform functionable instances of functionables values
        futures.add(this.executorService.submit(() -> {
            server.getFunctionableRunner().distributeValues();
        }));

        log.info("[ExecutionManager] futures added server.getFunctionableRunner().distributeValues()");

        List<Vertex<T>> vertices = this.server.getVertexGroup().getVertices();
        int activeVertexCount = 0;
        for (Vertex<T> vertex : vertices) {
            if (vertex.getStatus() == Vertex.VertexStatus.ACTIVE) {
                activeVertexCount++;
            }
        }

        CountDownLatch computeLatch = new CountDownLatch(activeVertexCount);
        // start the computation of the vertices
        for (Vertex<T> vertex : vertices) {
            if (vertex.getStatus() == Vertex.VertexStatus.ACTIVE) {
                futures.add(executorService.submit(() -> {
                    vertex.step();
                    computeLatch.countDown();
                }));
            }
        }

        log.info("[ExecutionManager] futures added vertex.step()");

        // TODO: Set all the vertices inactive
        if (server.getInMailQueue().isEmpty()) {
            for (Vertex<T> vertex : vertices) {
                vertex.setStatus(Vertex.VertexStatus.INACTIVE);
            }
        }

        computeLatch.await(); // block until all computing tasks are finished

        log.info("[ExecutionManager] futures added vertex.setStatus INACTIVE");

        // execute functionables
        CountDownLatch functionableLatch = new CountDownLatch(1);
        futures.add(executorService.submit(() -> {
            server.getFunctionableRunner().runFunctionableTasks(this.server);
            functionableLatch.countDown();
        }));

        functionableLatch.await(); // block until all functionable tasks are finished (future?)

        log.info("[ExecutionManager] finished runFunctionableTasks");

        CountDownLatch mailingLatch = new CountDownLatch(server.getOutMailQueue().size());
        // send the mail to the other servers
        while (!this.server.getOutMailQueue().isEmpty()) {
            Mail mail = this.server.getOutMailQueue().poll();
            futures.add(executorService.submit(() -> {
                int receiverId = this.server.findServerByVertexId(mail.getToVertexId());
                MessageProducer.produceWorkerServerMessage(mail, receiverId);
                mailingLatch.countDown();
            }));
        }
        mailingLatch.await(); // block until all mailing tasks are finished
        log.info("[ExecutionManager] finished mailing out");
    }
}