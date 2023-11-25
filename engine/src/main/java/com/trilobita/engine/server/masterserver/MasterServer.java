package com.trilobita.engine.server.masterserver;

import com.trilobita.core.graph.Graph;
import com.trilobita.engine.server.AbstractServer;
import com.trilobita.engine.server.masterserver.execution.ExecutionManager;
import com.trilobita.engine.server.masterserver.heartbeat.HeartbeatManager;
import com.trilobita.engine.server.masterserver.partition.Partitioner;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Master Server is the master of a server cluster, coordinate the start and the
 * end of a Superstep
 */
@Slf4j
@Getter
public class MasterServer<T> extends AbstractServer<T> {
    Graph<T> graph;                                         // the graph to be computed
    Partitioner<T> graphPartitioner;                        // the partitioner of the graph
    ExecutionManager<T> executionManager;                   // the superstep coordinator
    HeartbeatManager heartbeatManager;                      // the heartbeat manager
    @Setter
    List<Integer> workerIds = new ArrayList<>();            // the alive working servers' ids
    @Setter
    List<Integer> masterIds = new ArrayList<>();            // the alive master servers' ids
    public boolean isPrimary;

    public MasterServer(Partitioner<T> graphPartitioner, int nWorker, int id, int nReplica, int snapshotFrequency) {
        super(id, graphPartitioner.getPartitionStrategy()); // the standard server id of master is 0
        this.executionManager = new ExecutionManager<>(this, snapshotFrequency);
        this.heartbeatManager = new HeartbeatManager(this, this.workerIds, this.masterIds);
        this.graphPartitioner = graphPartitioner;
        for (int i = 0; i < nWorker; i++) {
            this.workerIds.add(i + 1);
        }
        for (int i = 0; i < nReplica; i++) {
            this.masterIds.add(i + 1);
        }
    }

    @Override
    public void start() {
        isPrimary = true;
        try {
            this.executionManager.listen();
            this.heartbeatManager.listen();
            this.executionManager.partitionGraph(workerIds);
        } catch (ExecutionException | InterruptedException  e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void pause() {
    }

    @Override
    public void shutdown() {
    }

    /**
     * load the graph to the master server
     * @param graph the graph to be loaded
     */
    public void setGraph(Graph<T> graph) {
        this.graph = graph;
    }
}
