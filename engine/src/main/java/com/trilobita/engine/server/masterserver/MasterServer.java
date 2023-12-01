package com.trilobita.engine.server.masterserver;

import com.trilobita.commons.Mail;
import com.trilobita.core.graph.Graph;
import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.core.messaging.MessageProducer;
import com.trilobita.engine.monitor.Monitor;
import com.trilobita.engine.monitor.metrics.Metrics;
import com.trilobita.engine.server.AbstractServer;
import com.trilobita.engine.server.masterserver.execution.ExecutionManager;
import com.trilobita.engine.server.masterserver.heartbeat.HeartbeatManager;
import com.trilobita.engine.server.masterserver.partition.Partitioner;
import com.trilobita.engine.server.util.functionable.Functionable;
import com.trilobita.engine.server.util.functionable.FunctionableRunner.MasterFunctionableRunner;
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
    int snapshotFrequency;
    @Setter
    List<Integer> workerIds = new ArrayList<>();            // the alive working servers' ids
    @Setter
    List<Integer> masterIds = new ArrayList<>();            // the alive master servers' ids
    public boolean isPrimary;
    MasterFunctionableRunner masterFunctionableRunner;

    public MasterServer(Partitioner<T> graphPartitioner, int nWorker, int id, int nReplica, int snapshotFrequency, boolean isPrimary) throws ExecutionException, InterruptedException {
        super(id, graphPartitioner.getPartitionStrategy()); // the standard server id of master is 0
        for (int i = 0; i < nWorker; i++) {
            this.workerIds.add(i + 1);
        }
        for (int i = 0; i < nReplica; i++) {
            this.masterIds.add(i + 1);
        }
        this.graphPartitioner = graphPartitioner;
        this.snapshotFrequency = snapshotFrequency;
        this.isPrimary = isPrimary;
        this.executionManager = new ExecutionManager<>(this, snapshotFrequency);
        this.heartbeatManager = new HeartbeatManager(this, this.workerIds, this.masterIds);
        this.masterFunctionableRunner = MasterFunctionableRunner.getInstance();
    }

    @Override
    public void start() {
        try {
            Monitor.start();
            Metrics.setMasterStartTime();
            this.executionManager.listen();
            this.heartbeatManager.listen();
            this.executionManager.partitionGraph(workerIds);
            this.sendfunctionables();
        } catch (ExecutionException | InterruptedException  e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void pause() throws InterruptedException {
        this.executionManager.stop();
    }

    @Override
    public void shutdown() throws InterruptedException {
        //  todo: send message to all replica and workers (the same topic)
        Monitor.stop();
        Metrics.computeMasterDuration();
        Monitor.masterStore("data/performance/master"+this.serverId);
        MessageProducer.createAndProduce(null, new Mail(), "STOP");
        this.executionManager.stop();
        this.heartbeatManager.stop();
    }

    /**
     * load the graph to the master server
     * @param graph the graph to be loaded
     */
    public void setGraph(Graph<T> graph) {
        this.graph = graph;
        Metrics.Superstep.initialize();
        for (Vertex<T> vertex : graph.getVertices()) {
            Metrics.Superstep.incrementVertexNum(1);
            Metrics.Superstep.incrementEdgeNum(vertex.getEdges().size());
        }
    }

    /**
     * Register functionables to masterFunctionableRunner
     *
     * @param functionables functionable sets
     */
    public void setFunctionables(Functionable.FunctionableRepresenter[] functionables) {
        if (functionables != null) {
            this.masterFunctionableRunner.registerFunctionables(functionables);
        }
    }

    /**
     * Send regitsered functionable instances to all working servers to their
     * message topics.
     */
    private void sendfunctionables() {
        this.masterFunctionableRunner.broadcastFunctionables();
    }

}
