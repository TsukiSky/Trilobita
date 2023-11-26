package com.trilobita.engine.server.masterserver.heartbeat.checker;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * HeartbeatChecker is used to check the heartbeats of the workers
 */
@Data
@Slf4j
public class HeartbeatChecker {
    private int serverId;
    private Boolean isWorkerChecker;
    private FaultHandler faultHandler;
    private Boolean isHandlingFault = false;
    private HashMap<Integer, Boolean> heartbeats = new HashMap<>();
    private final ScheduledExecutorService heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();

    public HeartbeatChecker(int serverId, List<Integer> targetIds, boolean isWorkerChecker, FaultHandler faultHandler){
        this.serverId = serverId;
        targetIds.forEach(id -> this.heartbeats.put(id, false));
        this.isWorkerChecker = isWorkerChecker;
        this.faultHandler = faultHandler;
    }

    public void recordHeartbeatFrom(int id){
        heartbeats.put(id, true);
    }

    public void check(){
        if (isHandlingFault) {
            return;
        }
        if (isWorkerChecker) {
            // check the heartbeats of the workers
            List<Integer> errors = new ArrayList<>();
            log.info("{}",heartbeats);
            heartbeats.forEach((id, heartbeat) -> {
                if (!heartbeat) {
                    // heartbeat is not detected
                    errors.add(id);
                }
                heartbeats.put(id, false);  // reset the heartbeat
            });
            if (errors.size() > 0) {
                log.info("found errr");
                isHandlingFault = true;
                faultHandler.handleFault(errors);
            }
        } else {
            // check the heartbeat of the master
            isHandlingFault = true;
            heartbeats.forEach((id, heartbeat) -> {
                if (heartbeat && id > serverId) {
                    // a heartbeat from a master server with a larger id is detected
                    isHandlingFault = false;
                }
                heartbeats.put(id, false);  // reset the heartbeat
            });

            if (isHandlingFault) {
                faultHandler.handleFault(new ArrayList<>());
                isHandlingFault = false;
            }
        }
    }

    public interface FaultHandler {
        void handleFault(List<Integer> ids);
    }

    public void start(){
        if (!isHandlingFault) {
            heartbeatExecutor.scheduleAtFixedRate(this::check, 5, 1, TimeUnit.SECONDS);
            log.info("Heartbeat checking service will start in 5 seconds.");
        } else {
            log.info("Heartbeat checking service is already running.");
        }
    }

    public void stop() throws InterruptedException {
        heartbeatExecutor.shutdown();
        heartbeatExecutor.awaitTermination(1000, TimeUnit.MICROSECONDS);
    }
}
