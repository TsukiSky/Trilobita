package com.trilobita.engine.server.masterserver;

import com.trilobita.commons.Computable;
import com.trilobita.core.graph.Graph;
import com.trilobita.core.graph.vertex.Vertex;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Data
public class ValueSnapshot<T> {
    private final ConcurrentHashMap<Integer, Computable<T>> snapshot;
    private Graph<T> graph;
    public ValueSnapshot(){
        this.snapshot = new ConcurrentHashMap<>();
    }

    public void record(HashMap<Integer, Computable<T>> vertexValue){
        Set<Map.Entry<Integer, Computable<T>>> set = vertexValue.entrySet();
        for (Map.Entry<Integer, Computable<T>> entry: set){
            snapshot.put(entry.getKey(), entry.getValue());
        }
    }

    public void finishSuperstep(Graph<T> graph){
        for (Vertex<T> v: graph.getVertices()){
            v.setValue(this.snapshot.get(v.getId()));
        }
    }
}
