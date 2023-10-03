package com.trilobita.engine.server.masterserver;

import com.trilobita.commons.Address;
import com.trilobita.core.graph.Graph;
import com.trilobita.engine.server.AbstractServer;
import com.trilobita.engine.server.masterserver.partitioner.AbstractPartitioner;
import com.trilobita.engine.server.workerserver.WorkerServer;
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
    private static final MasterServer instance = new MasterServer(0, new Address());    // Singleton implementation of MasterServer TODO: add the method of MasterServer initialization

    private MasterServer(int serverId, Address address) {
        super(serverId, address);
    }

    @Override
    public void initialize() {}

    @Override
    public void start() {}

    @Override
    public void pause() {}

    @Override
    public void shutdown() {}

    public void onCompleteSignal(Integer workerId) {}

    public void sendStartSignal() {}

    public void partitionGraph() {}
}
