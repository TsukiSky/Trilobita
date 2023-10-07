package com.trilobita.engine.server.masterserver;

import com.trilobita.core.graph.Graph;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.engine.server.AbstractServer;
import com.trilobita.engine.server.masterserver.partitioner.AbstractPartitioner;
import com.trilobita.engine.server.masterserver.partitioner.HashPartitioner;
import com.trilobita.engine.server.workerserver.WorkerServer;

import java.util.ArrayList;

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
    ArrayList<VertexGroup> vertexGroupArrayList;


    private static MasterServer instance;

    private MasterServer(int serverId) {
        super(serverId);
    }

    public static synchronized MasterServer getInstance() {
        if (instance == null) {
            instance = new MasterServer(0);
            instance.initialize();
        }
        return instance;
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

    public void partitionGraph(Graph graph, Integer nWorkers) {
        AbstractPartitioner partitioner = new HashPartitioner(nWorkers);
        this.vertexGroupArrayList = partitioner.Partition(graph, nWorkers);
    }
}
