package com.trilobita.server.masterserver;

import com.trilobita.server.AbstractServer;
import com.trilobita.server.masterserver.partitioner.AbstractPartitionner;
import com.trilobita.server.workerserver.WorkerServer;

public class MasterServer extends AbstractServer {
    //    Graph graph;  // TODO: Add Graph
    AbstractPartitionner graphPartitioner;
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
