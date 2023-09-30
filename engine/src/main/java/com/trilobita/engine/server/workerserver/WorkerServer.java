package com.trilobita.engine.server.workerserver;

import com.trilobita.commons.Mail;
import com.trilobita.engine.server.AbstractServer;
import com.trilobita.engine.util.Hardware;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Worker Server controls vertex computations and communicate with other servers
 */
public class WorkerServer extends AbstractServer {
    private final ExecutorService executorService;

    public WorkerServer() {
        this.executorService = Executors.newFixedThreadPool(Hardware.getCoreNum()); // number of thread is the number of cores on current machine by default
    }


    public WorkerServer(int numOfExecutor) {
        this.executorService = Executors.newFixedThreadPool(numOfExecutor);
    }

    @Override
    public void initialize() {

    }

    @Override
    public void start() {
    }

    @Override
    public void shutdown() {

    }

    public void onStartSignal() {

    }

    public void sendCompleteSignal() {

    }

    public void distributeMail(Mail mail) {

    }
}
