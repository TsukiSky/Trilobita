package com.trilobita.engine.server.masterserver.partitioner;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * This class implements the PartitionStrategy interface and represents an index-based partitioning strategy.
 */
public class IndexPartitionStrategy implements PartitionStrategy, Serializable {
    private final int graphSize;  // The graph to be partitioned.
    private List<Integer> workerIdList;    // The number of workers (servers) available for partitioning.

    /**
     * Constructor to initialize the IndexPartitionStrategy with the graph and the number of workers.
     *
     * @param graphSize    The graph's size.
     * @param nWorkers The number of workers/servers to which the partitioning will be distributed.
     */
    public IndexPartitionStrategy(int graphSize, int nWorkers) {
        this.graphSize = graphSize;
        this.workerIdList = new ArrayList<>();
        for (int i=1;i<=nWorkers;i++){
            this.workerIdList.add(i);
        }
    }

    /**
     * Get the server ID based on an index-based partitioning strategy using the given vertex ID.
     *
     * @param vertexId The vertex ID for which the server ID is determined.
     * @return The server ID to which the given vertex should be assigned based on the index-based strategy.
     */
    @Override
    public int getServerIdByVertexId(int vertexId) {
        int verticesPerWorker = (int) Math.ceil((double) graphSize / workerIdList.size());

        // Determine the server ID by dividing vertices into worker groups based on index.
        for (int i = 0; i < workerIdList.size(); i++) {
            if (vertexId <= verticesPerWorker * (i + 1)) {
                return workerIdList.get(i);
            }
        }

        // If the vertex ID does not fit into any worker group, return 0.
        return 0;
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
