package com.trilobita.server.masterserver.partitioner;

import com.trilobita.core.graph.Graph;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.core.graph.vertex.AbstractVertex;
import java.util.ArrayList;
import java.util.List;

public class Partitioner extends AbstractPartitioner {
    @Override
    public ArrayList<VertexGroup> Partition(Graph graph, Integer nWorkers) {
        ArrayList<VertexGroup> arrayList = this.PartitionByHash(graph,nWorkers);
        return  arrayList;
    }

    private ArrayList<VertexGroup> PartitionByHash(Graph graph, Integer nWorkers){
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

    private ArrayList<VertexGroup> PartitionByIndex(Graph graph, Integer nWorkers) {
        List<AbstractVertex> vertexList = graph.getVertexSet();
        ArrayList<VertexGroup> arrayList = new ArrayList<>(nWorkers);
        int verticesPerWorker = vertexList.size() / nWorkers;
        int extraVertices = vertexList.size() % nWorkers;
        int currentIndex = 0;
        for (int i = 0; i < nWorkers; i++) {
            int verticesForThisWorker = verticesPerWorker;
            if (extraVertices > 0) {
                verticesForThisWorker++;
                extraVertices--;
            }
            List<AbstractVertex> verticesForWorker = vertexList.subList(currentIndex, currentIndex + verticesForThisWorker);
            Graph worker = new Graph(new ArrayList<>(verticesForWorker));
            currentIndex += verticesForWorker.size();
            arrayList.add(worker);
        }
        return arrayList;
    }
}
