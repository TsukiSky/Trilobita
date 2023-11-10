package com.trilobita.engine.server.masterserver.partitioner;

import java.io.Serializable;

/**
 * This class implements the PartitionStrategy interface and represents a hash-based partitioning strategy.
 */
public class HashPartitionStrategy implements PartitionStrategy, Serializable {
    private final int nWorkers; // The number of workers (servers) available for partitioning.

    /**
     * Constructor to initialize the HashPartitionStrategy with the number of workers.
     *
     * @param nWorkers The number of workers/servers to which the partitioning will be distributed.
     */
    public HashPartitionStrategy(int nWorkers) {
        this.nWorkers = nWorkers;
    }

    /**
     * Get the server ID by applying a hash-based partitioning strategy to the given vertex ID.
     *
     * @param vertexId The vertex ID for which the server ID is determined.
     * @return The server ID to which the given vertex should be assigned based on the hash-based strategy.
     */
    @Override
    public int getServerIdByVertexId(int vertexId) {
        int serverId;
        // Calculate the server ID based on the vertex ID using a hash-based approach.
        if (vertexId % nWorkers == 0) {
            serverId = nWorkers;
        } else {
            serverId = vertexId % nWorkers;
        }
        return serverId;
    }
}