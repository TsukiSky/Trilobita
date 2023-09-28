package com.trilobita.server.masterserver;

import com.trilobita.core.graph.Graph;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.server.AbstractServer;
import com.trilobita.server.masterserver.partitioner.AbstractPartitioner;
import com.trilobita.server.masterserver.partitioner.Partitioner;
import com.trilobita.server.workerserver.WorkerServer;

import java.util.ArrayList;

public class MasterServer extends AbstractServer {
    Graph graph;
    AbstractPartitioner graphPartitioner;
    WorkerServer[] workerCluster;

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    public void onCompleteSignal(Integer workerId) {

    }

    public void sendStartSignal() {

    }

    public void partitionGraph(Graph graph, Integer nWorkers) {
        ArrayList<VertexGroup> vertexGroupArrayList;
        AbstractPartitioner partitioner = new Partitioner();
        vertexGroupArrayList = partitioner.Partition(graph, nWorkers);
    }
}
