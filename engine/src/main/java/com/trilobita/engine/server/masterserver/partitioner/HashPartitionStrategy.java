package com.trilobita.engine.server.masterserver.partitioner;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * This class implements the PartitionStrategy interface and represents a hash-based partitioning strategy.
 */
public class HashPartitionStrategy implements PartitionStrategy, Serializable {
    private List<Integer> workerIdList; // The number of workers (servers) available for partitioning.

    /**
     * Constructor to initialize the HashPartitionStrategy with the number of workers.
     *
     * @param nWorkers The number of workers/servers to which the partitioning will be distributed.
     */
    public HashPartitionStrategy(int nWorkers) {
        this.workerIdList = new ArrayList<>();
        for (int i=1;i<=nWorkers;i++){
            this.workerIdList.add(i);
        }
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
        if (vertexId % workerIdList.size() == 0) {
            serverId = workerIdList.size();
        } else {
            serverId = vertexId % workerIdList.size();
        }
        return workerIdList.get(serverId-1);
    }

    @Override
    public List<Integer> getWorkerIdList() {
        return this.workerIdList;
    }

    @Override
    public void setWorkerIdList(List<Integer> workerIdList) {
        this.workerIdList = workerIdList;
    }
}