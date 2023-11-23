package com.trilobita.engine.server.masterserver;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trilobita.commons.*;
import com.trilobita.core.graph.Graph;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.core.messaging.MessageConsumer;
import com.trilobita.core.messaging.MessageProducer;
import com.trilobita.engine.server.AbstractServer;
import com.trilobita.engine.server.heartbeat.HeartbeatChecker;
import com.trilobita.engine.server.heartbeat.HeartbeatSender;
import com.trilobita.engine.server.masterserver.partitioner.Partitioner;
import com.trilobita.engine.server.masterserver.util.Snapshot;
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
    MessageConsumer confirmReceiveConsumer;
    ConcurrentHashMap<Integer, Boolean> workerStatus; // the status of the workers
    HeartbeatChecker workerHeartbeatChecker;
    HeartbeatChecker masterHeartbeatChecker;
    HeartbeatSender heartbeatSender;
    List<Snapshot<T>> snapshots;
    ConcurrentHashMap<Integer, Boolean> confirmMessage;
    ConcurrentHashMap<Integer, Boolean> finished;
    int snapshotFrequency = 5; // whether the server is the master
    List<Integer> aliveWorkerIds; // the alive working servers' ids
    List<Integer> masterIds; // the alive master servers' ids
    volatile boolean isWorking;

    public MasterServer(Partitioner<T> graphPartitioner, int nWorker, int id, int nReplica, int snapshotFrequency)
            throws ExecutionException, InterruptedException {
        super(id, graphPartitioner.getPartitionStrategy()); // the standard server id of master is 0
        this.confirmMessage = new ConcurrentHashMap<>();
        this.finished = new ConcurrentHashMap<>();
        this.graphPartitioner = graphPartitioner;
        this.nFinishedWorker = new AtomicInteger(0);
        this.heartbeatSender = new HeartbeatSender(getServerId(), false);
        this.aliveWorkerIds = new ArrayList<>();
        this.snapshots = new ArrayList<>();
        this.snapshotFrequency = snapshotFrequency;
        for (int i = 0; i < nWorker; i++) {
            this.aliveWorkerIds.add(i + 1);
            this.confirmMessage.put(i + 1, false);
            this.finished.put(i + 1, false);
        }
        this.masterIds = new ArrayList<>();
        for (int i = 0; i < nReplica; i++) {
            this.masterIds.add(i + 1);
        }
        this.confirmReceiveConsumer = new MessageConsumer("CONFIRM_RECEIVE", this.getServerId(), new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail value, int partition, long offset) throws JsonProcessingException, InterruptedException, ExecutionException {
                if (!isWorking) {
                    return;
                }

                int workerId = (int) value.getMessage().getContent();
                confirmMessage.put(workerId, true);

//               check if all are true, set all to false and send confirm start signal
                boolean flag = true;
                Set<Map.Entry<Integer, Boolean>> set = confirmMessage.entrySet();
                for (Map.Entry<Integer, Boolean> entry: set){
                    if (!entry.getValue()){
                        flag = false;
                        break;
                    }
                }

                if (!flag){
                    log.info("received confirm receive message from worker {}", workerId);
                }

                if (flag) {
//                 send start message to all workers
                    for (Map.Entry<Integer, Boolean> entry: set){
                       entry.setValue(false);
                    }
                    MessageProducer.createAndProduce(null, new Mail(), "CONFIRM_START");
                    workerHeartbeatChecker.setIsProcessing(false);
                }
            }
        });

        this.workerHeartbeatChecker = new HeartbeatChecker(this.aliveWorkerIds, true, this.getServerId(),
                new HeartbeatChecker.FaultHandler() {
                    @Override
                    public void handleFault(List<Integer> errList) {
                        if (!MasterServer.this.isWorking) {
                            return;
                        }
                        for (int id: errList){
                            log.info("[Fault] detected server {} is down, start repartitioning...", id);
                            aliveWorkerIds.remove((Integer) id);
                            confirmMessage.remove(id);
                            finished.remove(id);
                            nFinishedWorker.set(0);
                        }

                        MasterServer.this.partitionGraph();
                        log.info("finished repartitioning...");
                    }
                });

        this.masterHeartbeatChecker = new HeartbeatChecker(this.masterIds, false, this.getServerId(),
                new HeartbeatChecker.FaultHandler() {
                    @Override
                    public void handleFault(List<Integer> errList) {
                        if (isWorking) {
                            return;
                        }
                        // if all id with greater id has died, become the master
                        log.info("[Fault] detected current master is down, trying to become master...");
                        isWorking = true;
                        MasterServer.this.partitionGraph();
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
                        Map<String, Object> map = (Map<String, Object>) value.getMessage().getContent();
                        HashMap<Integer, Computable<T>> vertexValues = (HashMap<Integer, Computable<T>>) map.get("VERTEX_VALUES");

                        if (vertexValues.size()>0) {
                            // update the graph
                            boolean finish = (boolean) map.get("FINISH");
                            int workerId = (int) map.get("ID");
                            graph.updateVertexValues(vertexValues);
                            log.info("[Graph] graph is : {}", graph);
                            finished.put(workerId, finish);
                        }
                        int nFinishedWorkers = nFinishedWorker.addAndGet(1);
                        log.info("[Superstep] number of finished workers: " + nFinishedWorkers);
                        if (nFinishedWorkers == aliveWorkerIds.size()) {
                            // start a new superstep
                            if (MasterServer.this.isDoingSnapshot()) {
                                // do snapshot
                                MasterServer.this.snapshotAndSync();
                                // check whether all workers have finished
                                boolean flag = true;
                                Set<Map.Entry<Integer, Boolean>> set = finished.entrySet();
                                for (Map.Entry<Integer, Boolean> entry: set) {
                                    if (!entry.getValue()){
                                        flag = false;
                                        break;
                                    }
                                }
                                if (flag){
                                    log.info("[Complete] the work has complete, the final graph is: {}", graph);
                                    return;
                                }
                            }
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
                        if (!aliveWorkerIds.contains(id)){
                            aliveWorkerIds.add(id);
                            confirmMessage.put(id, false);
                            finished.put(id, false);
                            workerHeartbeatChecker.getHeartbeatMap().put(id, true);
                            MasterServer.this.partitionGraph();
                        } else {
                            workerHeartbeatChecker.setHeatBeat(id);
                        }
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

        graphConsumer = new MessageConsumer("MASTER_SYNC", getServerId(), new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail value, int partition, long offset) {
                Map<String, Object> objectMap = (Map<String, Object>) value.getMessage().getContent();
                MasterServer.this.graph = (Graph<T>) objectMap.get("GRAPH");
                MasterServer.this.aliveWorkerIds = (List<Integer>) objectMap.get("ALIVE_WORKER_IDS");
            }
        });

        this.graphConsumer.start();
        this.confirmReceiveConsumer.start();
        this.completeSignalConsumer.start();
        this.workerHeatBeatConsumer.start();
        this.masterHeatBeatConsumer.start();
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
        Snapshot<T> snapshot = Snapshot.createSnapshot(superstep, this.superstep, graph);
        snapshot.store();
        this.snapshots.add(snapshot);
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
