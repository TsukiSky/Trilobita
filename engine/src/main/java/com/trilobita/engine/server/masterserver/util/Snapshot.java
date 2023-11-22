package com.trilobita.engine.server.masterserver.util;

import com.trilobita.commons.Computable;
import com.trilobita.core.graph.Graph;
import com.trilobita.core.graph.vertex.Vertex;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;

/**
 * The snapshot of the graph
 * @param <T>
 */
@Slf4j
@Data
public class Snapshot<T> {
    private final int id;
    private final HashMap<Integer, Computable<T>> vertexValues = new HashMap<>();

    public Snapshot(int id, Graph<T> graph) {
        this.id = id;
        for (Vertex<T> v : graph.getVertices()) {
            this.vertexValues.put(v.getId(), v.getValue());
        }
    }

    /**
     * Store the snapshot to the disk
     */
    public void store() {
        // TODO: implement this method after finalizing the JSON structure
    }

    /**
     * Create a snapshot of the graph
     * @param snapshotId the id of the snapshot
     * @param graph the graph to be snapshot
     * @return the snapshot
     * @param <T> the type of the vertex value
     */
    public static <T> Snapshot<T> createSnapshot(int snapshotId, Graph<T> graph) {
        log.info("[Snapshot] creating snapshot...");
        // shot the graph
        Snapshot<T> snapshot = new Snapshot<>(snapshotId, graph);

        for (Vertex<T> v : graph.getVertices()) {
            snapshot.vertexValues.put(v.getId(), v.getValue());
        }
        return snapshot;
    }
}
