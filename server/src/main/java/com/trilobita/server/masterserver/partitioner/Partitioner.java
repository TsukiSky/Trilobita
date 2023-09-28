package com.trilobita.server.masterserver.partitioner;

import com.trilobita.core.graph.Graph;
import com.trilobita.core.graph.Vertex.AbstractVertex;
import com.trilobita.core.graph.VertexGroup;

import java.util.ArrayList;
import java.util.List;

public class Partitioner extends AbstractPartitioner {

    @Override
    public ArrayList<VertexGroup> Partition(Graph graph, Integer nWorkers) {
        List<AbstractVertex> vertexList = graph.getVertexSet();
        ArrayList<VertexGroup> arrayList = new ArrayList<VertexGroup>(nWorkers);
        for (int i=0;i<vertexList.size();i++){
            AbstractVertex v = vertexList.get(i);
            List<AbstractVertex> currentWorker = arrayList.get((i+1)%nWorkers).getVertexSet();
            currentWorker.add(v);
            arrayList.get((i+1)%nWorkers).setVertexSet(currentWorker);
        }
        return arrayList;
    }
}
