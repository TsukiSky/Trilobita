package com.trilobita.engine.server.masterserver.partitioner;

import com.trilobita.core.graph.Graph;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.core.graph.vertex.Vertex;
import lombok.Data;


import java.util.ArrayList;
import java.util.List;

@Data
public abstract class AbstractPartitioner {

    public ArrayList<VertexGroup> Partition(Graph graph, int nWorkers){
        ArrayList<VertexGroup> arrayList = new ArrayList<>(nWorkers);
        for (int i = 0; i < nWorkers; i++) {
            arrayList.add(new VertexGroup());
        }
        List<Vertex> graphVertexSet = graph.getVertices();
        for (Vertex vertex : graphVertexSet) {
            int vertexId = vertex.getId();
            int serverId = getPartitionStrategy().getServerIdByVertexId(vertexId);
            arrayList.get(serverId - 1).getVertices().add(vertex);
        }
        return arrayList;
    }

    public abstract PartitionStrategy getPartitionStrategy();

    public abstract static class PartitionStrategy {
        public abstract int getServerIdByVertexId(int vertexId);
    }
}
