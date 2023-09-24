package com.trilobita.server.masterserver;

import com.trilobita.core.graph.Graph;
import com.trilobita.server.AbstractServer;
import com.trilobita.server.masterserver.partitioner.AbstractPartitioner;
import com.trilobita.server.workerserver.WorkerServer;

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

    public void partitionGraph() {

    }
}
