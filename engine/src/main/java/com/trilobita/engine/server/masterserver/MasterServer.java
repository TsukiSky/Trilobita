package com.trilobita.engine.server.masterserver;

import com.trilobita.commons.*;
import com.trilobita.core.graph.Graph;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.core.messaging.MessageConsumer;
import com.trilobita.core.messaging.MessageProducer;
import com.trilobita.engine.server.AbstractServer;
import com.trilobita.engine.server.heartbeat.HeartbeatChecker;
import com.trilobita.engine.server.heartbeat.HeartbeatSender;
import com.trilobita.engine.server.masterserver.partitioner.Partitioner;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Master Server is the master of a server cluster, coordinate the start and the
 * end of a Superstep
 */
@Slf4j
public class MasterServer<T> extends AbstractServer<T> {
    Graph<T> graph; // the graph to be computed
    Partitioner<T> graphPartitioner; // the partitioner of the graph
    AtomicInteger nFinishedWorker; // the number of workers that have finished the superstep
    MessageConsumer completeSignalConsumer; // the consumer that consume the finish signal from workers
    MessageConsumer workerHeatBeatConsumer;
    MessageConsumer masterHeatBeatConsumer;
    MessageConsumer graphConsumer;
    ConcurrentHashMap<Integer, Boolean> workerStatus; // the status of the workers
    HeartbeatChecker workerHeartbeatChecker;
    HeartbeatChecker masterHeartbeatChecker;
    HeartbeatSender heartbeatSender;
    List<Snapshot<T>> snapshots;
    int snapshotFrequency = 5; // whether the server is the master
    List<Integer> aliveWorkerIds; // the alive working servers' ids
    List<Integer> masterIds; // the alive master servers' ids
    volatile boolean isWorking;

    public MasterServer(Partitioner<T> graphPartitioner, int nWorker, int id, int nReplica)
            throws ExecutionException, InterruptedException {
        super(id, graphPartitioner.getPartitionStrategy()); // the standard server id of master is 0
        this.graphPartitioner = graphPartitioner;
        this.nFinishedWorker = new AtomicInteger(0);
        this.heartbeatSender = new HeartbeatSender(getServerId(), false);
        this.aliveWorkerIds = new ArrayList<>();
        this.snapshots = new ArrayList<>();
        for (int i = 0; i < nWorker; i++) {
            this.aliveWorkerIds.add(i + 1);
        }
        this.masterIds = new ArrayList<>();
        for (int i = 0; i < nReplica; i++) {
            this.masterIds.add(i + 1);
        }

        this.workerHeartbeatChecker = new HeartbeatChecker(this.aliveWorkerIds, true, this.getServerId(),
                new HeartbeatChecker.FaultHandler() {
                    @Override
                    public void handleFault(int id) {
                        if (!MasterServer.this.isWorking) {
                            return;
                        }
                        log.info("[Fault] detected server {} is down, start repartitioning...", id);

                        aliveWorkerIds.remove((Integer) id);
                        nFinishedWorker.set(0);
                        MasterServer.this.partitionGraph();
                        log.info("finished repartitioning...");
                    }
                });

        this.masterHeartbeatChecker = new HeartbeatChecker(this.masterIds, false, this.getServerId(),
                new HeartbeatChecker.FaultHandler() {
                    @Override
                    public void handleFault(int id) {
                        if (isWorking) {
                            return;
                        }
                        // if all id with greater id has died, become the master
                        log.info("[Fault] detected master {} is down, trying to become master...", id);
                        MasterServer.this.partitionGraph();
                        isWorking = true;
                    }
                });

        this.completeSignalConsumer = new MessageConsumer(Mail.MailType.FINISH_SIGNAL.ordinal(), super.getServerId(),
                new MessageConsumer.MessageHandler() {
                    @Override
                    public void handleMessage(UUID key, Mail value, int partition, long offset)
                            throws InterruptedException {
                        if (!MasterServer.this.isWorking || workerHeartbeatChecker.getIsProcessing()
                                || masterHeartbeatChecker.getIsProcessing()) {
                            return;
                        }
                        if (MasterServer.this.isDoingSnapshot()) {
                            // update the graph
                            HashMap<Integer, Computable<T>> vertexValues = (HashMap<Integer, Computable<T>>) value
                                    .getMessage().getContent();
                            graph.updateVertexValues(vertexValues);
                        }
                        int nFinishedWorkers = nFinishedWorker.addAndGet(1);
                        log.info("[Superstep] number of finished workers: " + nFinishedWorkers);
                        if (nFinishedWorkers == aliveWorkerIds.size()) {
                            // start a new superstep
                            if (MasterServer.this.isDoingSnapshot()) {
                                // do snapshot
                                MasterServer.this.snapshotAndSync();
                            }
                            log.info("[Graph] graph is : {}", graph.getVertices());
                            Thread.sleep(300);
                            superstep();
                        }
                    }
                });

        workerHeatBeatConsumer = new MessageConsumer("HEARTBEAT_WORKER", getServerId(),
                new MessageConsumer.MessageHandler() {
                    @Override
                    public void handleMessage(UUID key, Mail value, int partition, long offset) {
                        int id = (int) value.getMessage().getContent();
                        workerHeartbeatChecker.setHeatBeat(id);
                    }
                });

        masterHeatBeatConsumer = new MessageConsumer("HEARTBEAT_MASTER", getServerId(),
                new MessageConsumer.MessageHandler() {
                    @Override
                    public void handleMessage(UUID key, Mail value, int partition, long offset) {
                        int receivedMasterId = (int) value.getMessage().getContent();
                        // log.info("receiving heart beat from master {}", receivedMasterId);
                        if (receivedMasterId > MasterServer.this.serverId) {
                            isWorking = false;
                        }
                        masterHeartbeatChecker.setHeatBeat(receivedMasterId);
                    }
                });

        // TODO: Change the graph SYNC (with snapshot)
        graphConsumer = new MessageConsumer("MASTER_SYNC", getServerId(), new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail value, int partition, long offset) {
                Map<String, Object> objectMap = (Map<String, Object>) value.getMessage().getContent();
                MasterServer.this.graph = (Graph<T>) objectMap.get("GRAPH");
                MasterServer.this.aliveWorkerIds = (List<Integer>) objectMap.get("ALIVE_WORKER_IDS");
            }
        });

        this.graphConsumer.start();
        this.workerHeatBeatConsumer.start();
        this.masterHeatBeatConsumer.start();
        this.completeSignalConsumer.start();
        this.workerHeartbeatChecker.start();
        this.masterHeartbeatChecker.start();
        this.heartbeatSender.start();
    }

    @Override
    public void start() {
        isWorking = true;
        this.partitionGraph();
    }

    @Override
    public void pause() {
    }

    @Override
    public void shutdown() {
    }

    /**
     * start a new round of superstep
     */
    public void superstep() {
        this.superstep += 1;
        this.nFinishedWorker.set(0);
        MessageProducer.produceStartSignal(isDoingSnapshot());
    }

    /**
     * partition the graph and send the partitioned graph to the workers
     */
    public void partitionGraph() {
        if (this.graph == null) {
            throw new Error("graph is not set!");
        }
        Map<Integer, VertexGroup<T>> map;
        map = this.graphPartitioner.partition(graph, aliveWorkerIds);

        Set<Map.Entry<Integer, VertexGroup<T>>> set = map.entrySet();
        for (Map.Entry<Integer, VertexGroup<T>> entry : set) {
            Map<String, Object> objectMap = new HashMap<>();
            objectMap.put("PARTITION", entry.getValue());
            objectMap.put("PARTITION_STRATEGY", this.graphPartitioner.getPartitionStrategy());
            MessageProducer.producePartitionGraphMessage(objectMap, entry.getKey());
        }
    }

    /**
     * do snapshot and sync the graph with other masters
     */
    public void snapshotAndSync() {
        this.snapshots.add(Snapshot.createSnapshot(superstep, graph));
        this.syncGraph();
    }

    /**
     * sync the graph with other masters
     */
    private void syncGraph() {
        MessageProducer.produceSyncMessage(this.graph, this.aliveWorkerIds);
    }

    /**
     * check whether the server is doing snapshot
     * 
     * @return whether the server is doing snapshot
     */
    private boolean isDoingSnapshot() {
        return superstep % snapshotFrequency == 0;
    }

    /**
     * load the graph to the master server
     * 
     * @param graph the graph to be loaded
     */
    public void setGraph(Graph<T> graph) {
        this.graph = graph;
    }
}
