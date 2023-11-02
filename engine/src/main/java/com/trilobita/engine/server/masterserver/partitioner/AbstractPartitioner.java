package com.trilobita.engine.server.masterserver.partitioner;

import com.trilobita.core.graph.Graph;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.core.graph.vertex.Vertex;
import lombok.Data;


import java.util.ArrayList;
import java.util.List;

@Data
public abstract class AbstractPartitioner<T> {
    /**
     * Partition the graph into nWorker parts
     */
    public ArrayList<VertexGroup<T>> Partition(Graph<T> graph, int nWorker) {
        ArrayList<VertexGroup<T>> arrayList = new ArrayList<>(nWorker);
        for (int i = 0; i < nWorker; i++) {
            arrayList.add(new VertexGroup<>());
        }
        List<Vertex<T>> graphVertexSet = graph.getVertices();
        for (Vertex<T> vertex : graphVertexSet) {
            int vertexId = vertex.getId();
            int serverId = getPartitionStrategy().getServerIdByVertexId(vertexId);
            arrayList.get(serverId - 1).getVertices().add(vertex);
        }
        return arrayList;
    }

    /**
     * @return the partition strategy of the partitioner
     */
    public abstract PartitionStrategy getPartitionStrategy();

    /**
     * The partition strategy of the partitioner
     */
    public abstract static class PartitionStrategy {
        public abstract int getServerIdByVertexId(int vertexId);
    }
}
