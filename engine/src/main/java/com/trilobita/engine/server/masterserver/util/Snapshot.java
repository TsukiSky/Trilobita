package com.trilobita.engine.server.masterserver.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trilobita.core.graph.Graph;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;

/**
 * The snapshot of the graph
 * @param <T>
 */
@Slf4j
@Data
public class Snapshot<T> {
    private final int id;
    private final int superstep;
    private final Graph<T> graph;
    private final String snapshotDirectory = "data/snapshot/";

    private Snapshot(int id, int superstep, Graph<T> graph) {
        this.id = id;
        this.superstep = superstep;
        this.graph = graph;
    }

    /**
     * Store the snapshot to the disk
     */
    public void store() {
        File directory = new File(snapshotDirectory);
        if (!directory.exists()) {
            if (directory.mkdirs()) {
                log.info("[Snapshot] Snapshot directory data/snapshot created successfully");
            } else {
                log.info("[Snapshot] Failed to create snapshot directory");
            }
        }
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            objectMapper.writeValue(new File(directory, "snapshot_superstep_" + this.superstep + ".json"), this);
            log.info("[Snapshot] Snapshot stored");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Create a snapshot of the graph
     * @param snapshotId the id of the snapshot
     * @param graph the graph to be snapshot
     * @return the snapshot
     * @param <T> the type of the vertex value
     */
    public static <T> Snapshot<T> createSnapshot(int snapshotId, int superstep, Graph<T> graph) {
        return new Snapshot<>(snapshotId, superstep, graph);
    }
}
