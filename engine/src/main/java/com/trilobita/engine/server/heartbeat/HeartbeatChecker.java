package com.trilobita.engine.server.heartbeat;

import com.trilobita.core.messaging.MessageProducer;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class HeartbeatChecker extends Thread{
    private ConcurrentHashMap<Integer, Boolean> heartbeatMap;
    private Boolean running;


    public HeartbeatChecker(){
        running = true;
        heartbeatMap = new ConcurrentHashMap<>();
    }

    public int check(){
        Set<Map.Entry<Integer, Boolean>> set = heartbeatMap.entrySet();
        for (Map.Entry<Integer, Boolean> entry: set){
            if (!entry.getValue()){
                return entry.getKey();
            }
        }
        return -1;
    }
    @Override
    public void start(){
        while (running) {
            int failedId = check();
            if (failedId != -1){
//                todo: handle fault
            }
        }
    }

}
