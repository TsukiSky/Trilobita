package com.trilobita.engine.server.masterserver;

import com.trilobita.commons.Computable;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ValueSnapshot<T> {
    private final ConcurrentHashMap<Integer, Computable<T>> snapshot;
    public ValueSnapshot(){
        snapshot = new ConcurrentHashMap<>();
    }

    public void record(HashMap<Integer, Computable<T>> vertexValue){
        Set<Map.Entry<Integer, Computable<T>>> set = vertexValue.entrySet();
        for (Map.Entry<Integer, Computable<T>> entry: set){
            snapshot.put(entry.getKey(), entry.getValue());
        }
    }

    public ConcurrentHashMap<Integer, Computable<T>> getSnapshot(){
        return snapshot;
    }

}
