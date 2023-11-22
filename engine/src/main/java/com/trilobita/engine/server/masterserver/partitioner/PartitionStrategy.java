package com.trilobita.engine.server.masterserver.partitioner;

import java.util.List;

/**
 * This is an interface for defining the partitioning strategy used by the Master Server.
 */
public interface PartitionStrategy {
    /**
     * Get the server ID based on the vertex ID.
     *
     * @param vertexId The vertex ID for which the server ID is determined.
     * @return The server ID to which the given vertex should be assigned.
     */
    int getServerIdByVertexId(int vertexId);
    List<Integer> getWorkerIdList();
    void setWorkerIdList(List<Integer> nWorkers);
}
