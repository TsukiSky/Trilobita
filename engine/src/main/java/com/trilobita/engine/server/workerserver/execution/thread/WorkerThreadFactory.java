package com.trilobita.engine.server.workerserver.execution.thread;

import com.trilobita.engine.server.workerserver.WorkerServer;
import com.trilobita.engine.server.workerserver.execution.ExecutionManager;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;

public class WorkerThreadFactory<T> implements ThreadFactory {
    private final WorkerServer<T> server;
    private final ExecutionManager<T> executionManager;
    public CountDownLatch countDownLatch;

    public WorkerThreadFactory(WorkerServer<T> server, ExecutionManager<T> executionManager) {
        this.countDownLatch = new CountDownLatch(5);
        this.server = server;
        this.executionManager = executionManager;
    }

    @Override
    public Thread newThread(Runnable r) {
        return new WorkerThread<>(server, executionManager);
    }
}
