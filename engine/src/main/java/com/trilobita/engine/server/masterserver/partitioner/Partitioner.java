package com.trilobita.engine.server.masterserver.partitioner;

import com.trilobita.core.graph.Graph;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.core.graph.vertex.Vertex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Partitioner extends AbstractPartitioner {
    @Override
    public ArrayList<VertexGroup> Partition(Graph graph, Integer nWorkers) {
        return this.PartitionByHash(graph,nWorkers);
    }

    @Override
    public PartitionStrategy getPartitionStrategy() {
        return new PartitionStrategy() {
            @Override
            public int getServerIdByVertexId(int vertexId) {
                // TODO: implement get serverId by Vertex Id
                return super.getServerIdByVertexId(vertexId);
            }
        };
    }

    private ArrayList<VertexGroup> PartitionByHash(Graph graph, Integer nWorkers){
        List<Vertex> vertexList = graph.getVertexSet();
        ArrayList<VertexGroup> arrayList = new ArrayList<>(Collections.nCopies(nWorkers, new VertexGroup()));
        for (int i=0;i<vertexList.size();i++){
            Vertex v = vertexList.get(i);
            List<Vertex> currentWorker = arrayList.get((i+1)%nWorkers).getVertexSet();
            currentWorker.add(v);
            arrayList.get((i+1)%nWorkers).setVertexSet(currentWorker);
        }
        return arrayList;
    }

    private ArrayList<VertexGroup> PartitionByIndex(Graph graph, Integer nWorkers) {
        List<Vertex> vertexList = graph.getVertexSet();
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
            List<Vertex> verticesForWorker = vertexList.subList(currentIndex, currentIndex + verticesForThisWorker);
            Graph worker = new Graph(new ArrayList<>(verticesForWorker));
            currentIndex += verticesForWorker.size();
            arrayList.add(worker);
        }
        return arrayList;
    }
}
