package com.trilobita.engine.server.heartbeat;

import com.trilobita.core.messaging.MessageProducer;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class HeartbeatSender {
    private final ScheduledExecutorService heartbeatExecutor;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    private long delayed = 1; // in second
    private long period = 500; // in milliseconds

    private final int serverId;
    private final boolean isWorker;

    public HeartbeatSender(Integer serverId, boolean isWorker) {
        this.serverId = serverId;
        this.isWorker = isWorker;
        this.heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
    }

    public void start() {
        start(1, 1);
    }

    public void start(long delayed, long period) {
        this.delayed = delayed;
        this.period = period;
        if (isRunning.compareAndSet(false, true)) {
            heartbeatExecutor.scheduleAtFixedRate(this::sendHeartbeat, this.delayed, this.period, TimeUnit.MICROSECONDS);
            log.info("Heartbeat sending service will start in 1 second.");
        } else {
            log.info("Heartbeat sending service is already running.");
        }
    }


    private void sendHeartbeat() {
        MessageProducer.produceHeartbeatMessage(serverId, isWorker);
    }

    public void stop() {
        if (isRunning.compareAndSet(true, false)) {
            heartbeatExecutor.shutdown();
            try {
                if (!heartbeatExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    heartbeatExecutor.shutdownNow();
                }
                log.info("Heartbeat sending service stopped.");
            } catch (InterruptedException e) {
                heartbeatExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        } else {
            log.info("Heartbeat sending service is not running.");
        }
    }


    public void restart() {
        stop();
        start(this.delayed, this.period);
    }
}
