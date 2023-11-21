package com.trilobita.engine.server.heartbeat;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Data
@Slf4j
public class HeartbeatChecker extends Thread {
    private int serverId;
    private volatile boolean isRunning;
    private ConcurrentHashMap<Integer, Boolean> heartbeatMap;
    private volatile Boolean isProcessing;
    private FaultHandler faultHandler;
    private boolean checkWorker;
    private final ScheduledExecutorService heartbeatExecutor;


    public interface FaultHandler {
        void handleFault(int id);
    }

    public HeartbeatChecker(List<Integer> workerIds, boolean checkWorker, int serverId, FaultHandler faultHandler){
        this.serverId = serverId;
        this.isProcessing = false;
        this.heartbeatMap = new ConcurrentHashMap<>();
        for (int id: workerIds){
            this.heartbeatMap.put(id, true);
        }
        this.faultHandler = faultHandler;
        this.checkWorker = checkWorker;
        this.heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
    }

    public void setHeatBeat(int id){
        heartbeatMap.put(id, true);
    }

    public void check(){
        if (isProcessing){
            return;
        }
        if (isCheckWorker()) {
            Set<Map.Entry<Integer, Boolean>> set = heartbeatMap.entrySet();
            int id = -1;
            for (Map.Entry<Integer, Boolean> entry: set){
                if (!entry.getValue()){
                    id = entry.getKey();
                }
            }
            for (Map.Entry<Integer, Boolean> entry: set){
                heartbeatMap.put(entry.getKey(), false);
            }
            if (id != -1){
                isProcessing = true;
                faultHandler.handleFault(id);
                heartbeatMap.remove(id);
                isProcessing = false;
            }
        } else {
            Set<Map.Entry<Integer, Boolean>> set = heartbeatMap.entrySet();
            int id = serverId;
//            log.info("the master hashmap is: {}", heartbeatMap);
            boolean flag = true;
            for (Map.Entry<Integer, Boolean> entry: set){
                if (entry.getValue()){
                    if (entry.getKey() > id){
                        flag = false;
                        break;
                    }
                }
            }

            for (Map.Entry<Integer, Boolean> entry: set){
                heartbeatMap.put(entry.getKey(), false);
            }
            if (flag) {
                isProcessing = true;
                faultHandler.handleFault(id);
                isProcessing = false;
            }
        }
    }

    @Override
    public void run(){
        if (!isProcessing) {
            heartbeatExecutor.scheduleAtFixedRate(this::check, 5, 2, TimeUnit.SECONDS);
            log.info("Heartbeat checking service will start in 5 seconds.");
        } else {
            log.info("Heartbeat checking service is already running.");
        }
    }
}
