package com.trilobita.engine.server.masterserver.partition;

import com.trilobita.core.graph.Graph;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.engine.server.masterserver.partition.strategy.PartitionStrategy;
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class is responsible for partitioning a graph into multiple VertexGroups based on a given partitioning strategy.
 */
@Data
public class Partitioner<T> {
    protected final PartitionStrategy partitionStrategy;

    /**
     * Constructor to initialize the Partitioner with a partitioning strategy.
     *
     * @param partitionStrategy The strategy used for partitioning the graph.
     */
    public Partitioner(PartitionStrategy partitionStrategy) {
        this.partitionStrategy = partitionStrategy;
    }

    /**
     * Partition the graph into multiple VertexGroups based on the provided strategy.
     *
     * @param graph     The graph to be partitioned.
     * @param workerList  The number of workers (servers) to which the partitioning will be distributed.
     * @return An ArrayList of VertexGroup instances representing the partitioned graph.
     */
    public Map<Integer, VertexGroup<T>> partition(Graph<T> graph, List<Integer> workerList) {
        partitionStrategy.setWorkerIdList(workerList);
        Map<Integer, VertexGroup<T>> map = new HashMap<>();
        for (int i: workerList) {
            map.put(i, new VertexGroup<>());
        }
        List<Vertex<T>> graphVertexSet = graph.getVertices();
        for (Vertex<T> vertex : graphVertexSet) {
            int vertexId = vertex.getId();
            int serverId = partitionStrategy.getServerIdByVertexId(vertexId);
            map.get(serverId).getVertices().add(vertex);
        }
        return map;
    }
}
