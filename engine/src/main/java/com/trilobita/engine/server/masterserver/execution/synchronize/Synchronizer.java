package com.trilobita.engine.server.masterserver.execution.synchronize;

import com.trilobita.core.common.Mail;
import com.trilobita.core.common.Snapshot;
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
    private final MessageConsumer syncConsumer;
    private final List<Snapshot<T>> snapshots = new ArrayList<>();

    public Synchronizer(MasterServer<T> masterServer) {
        this.masterServer = masterServer;

        syncConsumer = new MessageConsumer("MASTER_SYNC", masterServer.getServerId(), new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail value, int partition, long offset) {
                Map<String, Object> objectMap = (Map<String, Object>) value.getMessage().getContent();
                Snapshot<T> snapshot = (Snapshot<T>) objectMap.get("SNAPSHOT");
                synchronize(snapshot);
            }
        });
    }

    /**
     * synchronize the graph with other masters
     * @param snapshot the snapshot to synchronize
     */
    public void synchronize(Snapshot<T> snapshot) {
        // TODO: SYNC should synchronize everything, not just the graph
        snapshot.store();
        masterServer.setGraph(snapshot.getGraph());
        masterServer.setWorkerIds(snapshot.getAliveWorkerIds());
        masterServer.setMailTable(snapshot.getMailTable());
        masterServer.getMasterFunctionableRunner().syncSnapshot(snapshot.getFunctionableValues());
    }

    /**
     * do snapshot and sync the graph with other masters
     */
    public void snapshotAndSync(Graph<T> graph) {
        log.info("[Snapshot] doing a snapshot");
        Snapshot<T> snapshot = Snapshot.createSnapshot(
                masterServer.getExecutionManager().getSuperstep(),
                masterServer.getExecutionManager().getSuperstep(),
                graph,
                this.masterServer.getWorkerIds(),
                masterServer.getExecutionManager().snapshotMailTable,
                masterServer.getMasterFunctionableRunner().createSnapshot()
        );
        snapshot.store();
        masterServer.getExecutionManager().snapshotMailTable.clear();
        this.snapshots.add(snapshot);
        this.sendSynchronizeMessage(snapshot);
    }

    /**
     * sync the graph with other masters
     */
    private void sendSynchronizeMessage(Snapshot<T> snapshot) {
        MessageProducer.produceSyncMessage(snapshot);
    }

    /**
     * Start the synchronizer
     */
    public void listen() throws ExecutionException, InterruptedException {
        syncConsumer.start();
    }

    public void stop() throws InterruptedException {
        syncConsumer.stop();
    }
}
