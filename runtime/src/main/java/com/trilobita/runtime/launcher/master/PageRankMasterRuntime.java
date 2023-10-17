package com.trilobita.runtime.launcher.master;

import com.trilobita.core.graph.Graph;
import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.engine.server.masterserver.MasterServer;
import com.trilobita.runtime.launcher.impl.PageRankVertex;

import java.util.ArrayList;
import java.util.List;

public class PageRankMasterRuntime {
    public static Graph createVertices(){
        PageRankVertex vertex1 = new PageRankVertex();
        vertex1.setId(1);

        PageRankVertex vertex2 = new PageRankVertex();
        vertex2.setId(2);

        PageRankVertex vertex3 = new PageRankVertex();
        vertex3.setId(3);

        PageRankVertex vertex4 = new PageRankVertex();
        vertex4.setId(4);

        PageRankVertex vertex5 = new PageRankVertex();
        vertex5.setId(5);

        PageRankVertex vertex6 = new PageRankVertex();
        vertex6.setId(6);

        List<Vertex> vertexList = new ArrayList<>();
        vertexList.add(vertex1);
        vertexList.add(vertex2);
        vertexList.add(vertex3);
        vertexList.add(vertex4);
        vertexList.add(vertex5);
        vertexList.add(vertex6);

        Graph graph = new Graph(vertexList);
        return graph;
    }
    public static void main(String[] args) {
        MasterServer masterServer = MasterServer.getInstance();
//        parse the graph
        Graph graph = PageRankMasterRuntime.createVertices();
        masterServer.initialize();
        masterServer.partitionGraph(graph, 10);
        masterServer.start();
    }
}
