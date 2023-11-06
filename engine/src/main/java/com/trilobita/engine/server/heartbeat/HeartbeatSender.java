package com.trilobita.engine.server.heartbeat;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class HeartbeatSender {

    private ScheduledExecutorService heartbeatExecutor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final List<String> workerServerIds;

    public HeartbeatSender(List<String> workerServerIds) {
        this.workerServerIds = workerServerIds;
        this.heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            heartbeatExecutor.scheduleWithFixedDelay(this::sendHeartbeats, 5, 1, TimeUnit.SECONDS);
            System.out.println("Heartbeat service will start in 5 seconds.");
        } else {
            System.out.println("Heartbeat service is already running.");
        }
    }


    private void sendHeartbeats() {
        for (String workerServerId : workerServerIds) {
            sendHeartbeat(workerServerId);
        }
    }

    private void sendHeartbeat(String workerServerId) {
        System.out.println("Sending heartbeat to worker server: " + workerServerId);
        // TODO: Implement the actual sending logic here
    }

    public void stop() {
        if (running.compareAndSet(true, false)) {
            heartbeatExecutor.shutdown();
            try {
                if (!heartbeatExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    heartbeatExecutor.shutdownNow();
                }
                System.out.println("Heartbeat service stopped.");
            } catch (InterruptedException e) {
                heartbeatExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        } else {
            System.out.println("Heartbeat service is not running.");
        }
    }


    public void restart() {
        stop();
        start();
    }
}
