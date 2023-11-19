package com.trilobita.engine.server.heartbeat;

import com.trilobita.core.messaging.MessageProducer;
import lombok.Data;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Data
public class HeartbeatChecker extends Thread{
    private ConcurrentHashMap<Integer, Boolean> heartbeatMap;
    private volatile Boolean isProcessing;
    private FaultHandler faultHandler;
    private boolean checkWorker;

    public interface FaultHandler {
        void handleFault();
    }

    public HeartbeatChecker(List<Integer> workerIds, boolean checkWorker, FaultHandler faultHandler){
        this.isProcessing = false;
        this.heartbeatMap = new ConcurrentHashMap<>();
        for (int id: workerIds){
            this.heartbeatMap.put(id, true);
        }
        this.faultHandler = faultHandler;
        this.checkWorker = checkWorker;
    }

    public int check(){
        Set<Map.Entry<Integer, Boolean>> set = heartbeatMap.entrySet();
        int index = -1;
        for (Map.Entry<Integer, Boolean> entry: set){
            if (!entry.getValue()){
                index = entry.getKey();
            }
        }
        for (Map.Entry<Integer, Boolean> entry: set){
            heartbeatMap.put(entry.getKey(), false);
        }
        return index;
    }

    @Override
    public void start(){
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (!isProcessing){
                int failedId = check();
                if (failedId != -1){
                    isProcessing = false;
                    faultHandler.handleFault();
                    isProcessing = true;
                }
            }

        }
    }

}
