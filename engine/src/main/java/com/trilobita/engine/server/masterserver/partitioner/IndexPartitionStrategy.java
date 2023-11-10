package com.trilobita.engine.server.masterserver.partitioner;

import com.trilobita.core.graph.Graph;
import com.trilobita.core.graph.vertex.Vertex;

import java.io.Serializable;
import java.util.List;

/**
 * This class implements the PartitionStrategy interface and represents an index-based partitioning strategy.
 */
public class IndexPartitionStrategy<T> implements PartitionStrategy, Serializable {
    private final int graphSize;  // The graph to be partitioned.
    private final int nWorkers;    // The number of workers (servers) available for partitioning.

    /**
     * Constructor to initialize the IndexPartitionStrategy with the graph and the number of workers.
     *
     * @param graphSize    The graph's size.
     * @param nWorkers The number of workers/servers to which the partitioning will be distributed.
     */
    public IndexPartitionStrategy(int graphSize, int nWorkers) {
        this.graphSize = graphSize;
        this.nWorkers = nWorkers;
    }

    /**
     * Get the server ID based on an index-based partitioning strategy using the given vertex ID.
     *
     * @param vertexId The vertex ID for which the server ID is determined.
     * @return The server ID to which the given vertex should be assigned based on the index-based strategy.
     */
    @Override
    public int getServerIdByVertexId(int vertexId) {
        int verticesPerWorker = (int) Math.ceil((double) graphSize / nWorkers);

        // Determine the server ID by dividing vertices into worker groups based on index.
        for (int i = 0; i < nWorkers; i++) {
            if (vertexId <= verticesPerWorker * (i + 1)) {
                return i + 1;
            }
        }

        // If the vertex ID does not fit into any worker group, return 0.
        return 0;
    }
}