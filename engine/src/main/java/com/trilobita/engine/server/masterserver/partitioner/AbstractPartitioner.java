package com.trilobita.engine.server.masterserver.partitioner;

import com.trilobita.core.graph.Graph;
import com.trilobita.core.graph.VertexGroup;

import java.util.ArrayList;

public abstract class AbstractPartitioner {
    public abstract ArrayList<VertexGroup> Partition(Graph graph, Integer nWorkers);

//    public abstract int getServerIdByVertexId(int vertexId);

    public abstract PartitionStrategy getPartitionStrategy();

    public abstract static class PartitionStrategy {
        public int getServerIdByVertexId(int vertexId) {
            return 0;
        }
    }
}
