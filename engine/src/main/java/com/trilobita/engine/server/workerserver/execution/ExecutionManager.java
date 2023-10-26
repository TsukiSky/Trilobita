package com.trilobita.engine.server.workerserver.execution;

import com.trilobita.engine.server.workerserver.WorkerServer;
import com.trilobita.engine.server.workerserver.execution.thread.WorkerThread;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * The execution manager on a single worker machine
 */
public class ExecutionManager<T> {
    private final List<WorkerThread<T>> workerThreads;
    public final LinkedBlockingQueue<Integer> activeVertices;

    public ExecutionManager(int parallelism, WorkerServer<T> server) {
        this.activeVertices = new LinkedBlockingQueue<>();
        this.workerThreads = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            WorkerThread<T> workerThread = new WorkerThread<>(server, this);
            workerThreads.add(workerThread);    // add worker thread to the list
        }
    }

    public void execute() {
        for (WorkerThread<T> workerThread : workerThreads) {
            workerThread.start();
        }
    }
}
