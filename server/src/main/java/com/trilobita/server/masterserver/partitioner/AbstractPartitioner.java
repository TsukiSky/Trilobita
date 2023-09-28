package com.trilobita.server.masterserver.partitioner;

import com.trilobita.core.graph.Graph;
import com.trilobita.core.graph.VertexGroup;

import java.util.ArrayList;

public abstract class AbstractPartitioner {
    public abstract ArrayList<VertexGroup> Partition(Graph graph, Integer nWorkers);
}
