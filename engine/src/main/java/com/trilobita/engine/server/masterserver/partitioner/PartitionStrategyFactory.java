package com.trilobita.engine.server.masterserver.partitioner;

import lombok.Data;


/**
 * Factory class for creating instances of partition strategies for a graph.
 * This determines how the graph will be divided among different workers.
 */
@Data
public class PartitionStrategyFactory {

    /**
     * Retrieves an instance of a partition strategy based on the type specified,
     * the number of worker nodes, and the graph to be partitioned.
     * The strategy defines how vertices will be distributed across worker nodes.
     *
     * @param type The type of partition strategy to use, which could be hash-based or index-based.
     * @param nWorkers The number of worker nodes among which the graph is to be partitioned.
     * @param graphSize The graph that needs to be partitioned.
     * @return An instance of the specified PartitionStrategy or null if the type is not recognized.
     */
    public PartitionStrategy getPartitionStrategy(String type, int nWorkers, int graphSize) {
        if ("hashPartitionStrategy".equalsIgnoreCase(type)) {
            return new HashPartitionStrategy(nWorkers);
        } else if ("indexPartitionStrategy".equalsIgnoreCase(type)) {
            return new IndexPartitionStrategy(graphSize, nWorkers);
        }
        return null;
    }
}
