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
public class Snapshot<T> {
    private final HashMap<Integer, Computable<T>> snapshot;
    public Snapshot(){
        this.snapshot = new HashMap<>();
    }

    public void record(HashMap<Integer, Computable<T>> vertexValues){
        snapshot.putAll(vertexValues);
    }

    public void finishSuperstep(Graph<T> graph){
        for (Vertex<T> v: graph.getVertices()){
            v.setValue(this.snapshot.get(v.getId()));
        }
    }
}
