package com.trilobita.runtime.launcher.master;

import com.trilobita.core.graph.Graph;
import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.engine.server.masterserver.MasterServer;
import com.trilobita.runtime.launcher.impl.PageRankVertex;

import java.util.ArrayList;
import java.util.List;

public class PageRankMasterRuntime {
    public static Graph createVertices(){
        List<Vertex> vertices = new ArrayList<>();
        PageRankVertex vertex0 = new PageRankVertex(0);
        vertices.add(vertex0);

        PageRankVertex vertex1 = new PageRankVertex(1);
        vertices.add(vertex1);

        PageRankVertex vertex2 = new PageRankVertex(2);
        vertices.add(vertex2);

        PageRankVertex vertex3 = new PageRankVertex(3);
        vertices.add(vertex3);

        PageRankVertex vertex4 = new PageRankVertex(4);
        vertex4.setId(4);
        vertices.add(vertex4);

        PageRankVertex vertex5 = new PageRankVertex(5);
        vertex5.setId(5);
        vertices.add(vertex5);

        PageRankVertex vertex6 = new PageRankVertex(6);
        vertex6.setId(6);
        vertices.add(vertex6);

        PageRankVertex vertex7 = new PageRankVertex(7);
        vertex7.setId(7);
        vertices.add(vertex7);

        PageRankVertex vertex8 = new PageRankVertex(8);
        vertex8.setId(8);
        vertices.add(vertex8);

        PageRankVertex vertex9 = new PageRankVertex(9);
        vertex9.setId(9);
        vertices.add(vertex9);

        vertex1.addEdge(vertex2);
        vertex2.addEdge(vertex4);
        vertex4.addEdge(vertex3);
        vertex3.addEdge(vertex4);
        vertex1.addEdge(vertex3);
        vertex7.addEdge(vertex3);
        vertex6.addEdge(vertex3);
        vertex9.addEdge(vertex3);
        vertex5.addEdge(vertex3);
        vertex7.addEdge(vertex8);
        vertex8.addEdge(vertex7);
        vertex0.addEdge(vertex8);
        vertex0.addEdge(vertex5);
        vertex0.addEdge(vertex9);
        vertex9.addEdge(vertex6);

        Graph graph = new Graph(vertices);
        return graph;
    }
    public static void main(String[] args) {
        MasterServer masterServer = MasterServer.getInstance();
//        parse the graph
        Graph graph = PageRankMasterRuntime.createVertices();
        masterServer.initialize();
        masterServer.partitionGraph(graph, 1);
        masterServer.start();
    }
}
