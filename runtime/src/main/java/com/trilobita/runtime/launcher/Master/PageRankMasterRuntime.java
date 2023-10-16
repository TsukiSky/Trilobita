package com.trilobita.runtime.launcher.Master;

import com.trilobita.core.graph.Graph;
import com.trilobita.engine.server.masterserver.MasterServer;

public class PageRankMasterRuntime {

    public static void main(String[] args) {
        MasterServer masterServer = MasterServer.getInstance();
//        parse the graph
        Graph graph = new Graph();
        masterServer.initialize();
        masterServer.partitionGraph(graph, 10);
        masterServer.start();
    }
}
