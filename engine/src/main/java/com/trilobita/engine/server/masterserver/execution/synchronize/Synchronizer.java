package com.trilobita.engine.server.masterserver.execution.synchronize;

import com.trilobita.commons.Mail;
import com.trilobita.core.graph.Graph;
import com.trilobita.core.messaging.MessageConsumer;
import com.trilobita.core.messaging.MessageProducer;
import com.trilobita.engine.server.masterserver.MasterServer;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Slf4j
public class Synchronizer<T> {
    private final MasterServer<T> masterServer;
    private final MessageConsumer graphConsumer;
    private final List<Snapshot<T>> snapshots = new ArrayList<>();

    public Synchronizer(MasterServer<T> masterServer) {
        this.masterServer = masterServer;

        graphConsumer = new MessageConsumer("MASTER_SYNC", masterServer.getServerId(), new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail value, int partition, long offset) {
                Map<String, Object> objectMap = (Map<String, Object>) value.getMessage().getContent();
                Graph<T> graph = (Graph<T>) objectMap.get("GRAPH");
                List<Integer> aliveWorkerIds = (List<Integer>) objectMap.get("ALIVE_WORKER_IDS");
                synchronize(graph, aliveWorkerIds);
            }
        });
    }

    /**
     * Synchronize the graph and alive worker ids among the master and replicas
     * @param graph the graph to be synchronized
     * @param aliveWorkerIds the alive worker ids
     */
    public void synchronize(Graph<T> graph, List<Integer> aliveWorkerIds) {
        // TODO: SYNC should synchronize everything, not just the graph
        masterServer.setGraph(graph);
        masterServer.setWorkerIds(aliveWorkerIds);
    }

    /**
     * do snapshot and sync the graph with other masters
     */
    public void snapshotAndSync(Graph<T> graph) {
        log.info("[Snapshot] doing a snapshot");
        Snapshot<T> snapshot = Snapshot.createSnapshot(masterServer.getSuperstep(), masterServer.getSuperstep(), graph);
        snapshot.store();
        this.snapshots.add(snapshot);
        this.syncGraph();
    }

    /**
     * sync the graph with other masters
     */
    private void syncGraph() {
        MessageProducer.produceSyncMessage(this.masterServer.getGraph(), this.masterServer.getWorkerIds());
    }

    /**
     * Start the synchronizer
     */
    public void listen() throws ExecutionException, InterruptedException {
        graphConsumer.start();
    }
}
