package com.trilobita.server.masterserver;

import com.trilobita.core.graph.Graph;
import com.trilobita.server.AbstractServer;
import com.trilobita.server.masterserver.partitioner.AbstractPartitioner;
import com.trilobita.server.workerserver.WorkerServer;
import lombok.Getter;

/**
 * Master Server is the master of a server cluster, coordinate the start and the end of a Superstep
 */
public class MasterServer extends AbstractServer {
    Graph graph;
    AbstractPartitioner graphPartitioner;
    WorkerServer[] workerCluster;
    Integer nRunningWorkers;
    Integer nPauseWorkers;
    Integer nDownWorkers;

    @Getter
    private static final MasterServer instance = new MasterServer();    // Singleton MasterServer

    private MasterServer() {}

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
