package com.trilobita.engine.server.masterserver.partitioner;

import com.trilobita.core.graph.Graph;
import com.trilobita.core.graph.vertex.Vertex;

import java.util.List;

public class IndexPartitioner extends AbstractPartitioner {

    int nWorkers;
    Graph graph;

    public IndexPartitioner(Graph graph,int nWorkers){
        this.graph = graph;
        this.nWorkers = nWorkers;
    }

    @Override
    public PartitionStrategy getPartitionStrategy() {
        return new PartitionStrategy() {
            @Override
            public int getServerIdByVertexId(int vertexId) {
                List<Vertex> vertexList = graph.getVertices();
                int verticesPerWorker = (int) Math.ceil((double) vertexList.size() / nWorkers);
                for(int i =0; i<nWorkers;i++){
                    if (vertexId<=verticesPerWorker*(i+1)){
                        return i+1;
                    }
                }
                return 0;
            }
        };
    }
}
