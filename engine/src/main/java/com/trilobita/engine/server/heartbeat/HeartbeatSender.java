package com.trilobita.engine.server.heartbeat;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class HeartbeatSender {

    private final ScheduledExecutorService heartbeatExecutor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final List<String> workerServerIds;

    public HeartbeatSender(List<String> workerServerIds) {
        this.workerServerIds = workerServerIds;
        this.heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            heartbeatExecutor.scheduleWithFixedDelay(this::sendHeartbeats, 5, 1, TimeUnit.SECONDS);
            log.info("Heartbeat service will start in 5 seconds.");
        } else {
            log.info("Heartbeat service is already running.");
        }
    }


    private void sendHeartbeats() {
        for (String workerServerId : workerServerIds) {
            sendHeartbeat(workerServerId);
        }
    }

    private void sendHeartbeat(String workerServerId) {
        log.info("Sending heartbeat to worker server: " + workerServerId);
        // TODO: Implement the actual sending logic here

    }

    public void stop() {
        if (running.compareAndSet(true, false)) {
            heartbeatExecutor.shutdown();
            try {
                if (!heartbeatExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    heartbeatExecutor.shutdownNow();
                }
                log.info("Heartbeat service stopped.");
            } catch (InterruptedException e) {
                heartbeatExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        } else {
            log.info("Heartbeat service is not running.");
        }
    }


    public void restart() {
        stop();
        start();
    }
}
