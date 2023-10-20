package com.trilobita.engine.server.workerserver.execution.thread;

import java.util.concurrent.CountDownLatch;

public class WorkerThread extends Thread {
    private final CountDownLatch latch;

    WorkerThread(CountDownLatch latch) {
        this.latch = latch;
    }

    @Override
    public void run() {

    }
}
