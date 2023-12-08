package com.trilobita.engine.server.masterserver.execution;

import com.trilobita.core.common.Computable;
import com.trilobita.core.common.Mail;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.core.messaging.MessageConsumer;
import com.trilobita.core.messaging.MessageProducer;
import com.trilobita.engine.monitor.Monitor;
import com.trilobita.engine.monitor.metrics.Metrics;
import com.trilobita.engine.server.masterserver.MasterServer;
import com.trilobita.engine.server.masterserver.execution.synchronize.Synchronizer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class ExecutionManager<T> {
    public final Map<Integer, List<Mail>> snapshotMailTable = new HashMap<>();
    private final MasterServer<T> masterServer;
    private final int snapshotFrequency;
    MessageConsumer completeSignalConsumer;
    MessageConsumer confirmStartConsumer;
    Synchronizer<T> synchronizer; // the synchronizer of the replicas
    private final AtomicInteger nFinishWorker = new AtomicInteger(0);
    private final AtomicInteger nCompleteWorker = new AtomicInteger(0);
    private final AtomicInteger nConfirmWorker = new AtomicInteger(0);
    @Getter
    private final AtomicInteger superstep = new AtomicInteger(0);

    private Integer superstepSnapshot = 0;

    public ExecutionManager(MasterServer<T> masterServer, int snapshotFrequency) {
        this.masterServer = masterServer;
        this.synchronizer = new Synchronizer<>(masterServer);
        this.snapshotFrequency = snapshotFrequency;
        initializeConsumers();
    }

    /**
     * start the execution manager
     */
    public void listen() throws ExecutionException, InterruptedException {
        this.confirmStartConsumer.start();
        this.completeSignalConsumer.start();
        this.synchronizer.listen();
        masterServer.messageConsumer.start();
    }

    public void stop() throws InterruptedException {
        this.confirmStartConsumer.stop();
        this.completeSignalConsumer.stop();
        this.synchronizer.stop();
        masterServer.messageConsumer.stop();
    }


    public void initializeConsumers() {
        // create the confirm start consumer
        this.confirmStartConsumer = new MessageConsumer("CONFIRM_RECEIVE", masterServer.getServerId(), new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail value, int partition, long offset) {
                if (!masterServer.isPrimary) {
                    return;
                }
                int senderId = (int) value.getMessage().getContent();
                log.info("[Signal] Received a confirm signal from Worker {}", senderId);
                nConfirmWorker.getAndAdd(1);
                if (nConfirmWorker.get() == masterServer.getWorkerIds().size()) {
                    // send start message to all workers
                    nFinishWorker.set(0);
                    nCompleteWorker.set(0);
                    nConfirmWorker.set(0);
                    masterServer.getHeartbeatManager().setIsHandlingFault(false);
                    MessageProducer.produce(null, new Mail(), "CONFIRM_START");
                }
            }
        });

        // check when a worker receives the message from other servers

        // create the complete signal consumer
        this.completeSignalConsumer = new MessageConsumer(Mail.MailType.FINISH_SIGNAL.ordinal(), masterServer.getServerId(), new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail value, int partition, long offset) throws InterruptedException {
                if (!masterServer.isPrimary) {
                    return;
                }
                if (Boolean.TRUE.equals(masterServer.getHeartbeatManager().getIsHandlingFault())) {
                    return;
                }
                // extract the content
                Map<String, Object> content = (Map<String, Object>) value.getMessage().getContent();
                HashMap<Integer, Computable<T>> vertexValues = (HashMap<Integer, Computable<T>>) content.get("VERTEX_VALUES");
                List<Mail> snapshotMails = (List<Mail>) content.get("SNAPSHOT_MAILS");
                boolean complete = (boolean) content.get("COMPLETE");
                Integer workerSuperstep = (Integer) content.get("SUPERSTEP");
                if (workerSuperstep > superstep.get()) {
                    return;
                }
                log.info("[Complete signal] {}", complete);
                if (complete) {
                    nCompleteWorker.addAndGet(1);
                }
                nFinishWorker.addAndGet(1);
                log.info("[Superstep] Number of finished workers: {}", nFinishWorker);

                if (!vertexValues.isEmpty()) {
                    // update the graph
                    masterServer.getGraph().updateVertexValues(vertexValues);
                    for (Mail snapshotMail : snapshotMails) {
                        snapshotMailTable.computeIfAbsent(snapshotMail.getToVertexId(), k -> new ArrayList<>());
                        snapshotMailTable.get(snapshotMail.getToVertexId()).add(snapshotMail);
                    }
                }

                if (nFinishWorker.get() == masterServer.getWorkerIds().size()) {
                    // aggregate functional values and send to workers
                    masterServer.getMasterFunctionableRunner().runFunctionableTasks();
                    Metrics.Superstep.computeMasterDuration();
                    Monitor.stopAndStartNewSuperstepMaster();
                    log.info("[Complete workers] {}", nCompleteWorker);
                    // check if the master needs to do a snapshot
                    if (isDoingSnapshot()) {
                        log.info("[Snapshot] Do Snapshot and Synchronization");
                        synchronizer.snapshotAndSync(masterServer.getGraph());
                        superstepSnapshot = superstep.get();
                    }
                    // check whether all workers have finished
                    if (nCompleteWorker.get() == masterServer.getWorkerIds().size()) {
                        log.info("[Complete] Task completes, the final graph is: {}", masterServer.getGraph());
                        masterServer.shutdown();
                    } else {
                        // start a new superstep
                        Thread.sleep(500);
                        Metrics.Superstep.setMasterSuperStepStartTime();
                        superstep();
                    }
                }
            }
        });
    }

    /**
     * start a new round of superstep
     */
    public void superstep() {
        log.info("#############################################################################################");
        log.info("[Superstep] Start Supertep");
        this.superstep.addAndGet(1);
        nFinishWorker.set(0);
        nCompleteWorker.set(0);
        MessageProducer.produceStartSignal(this.isDoingSnapshot());
    }

    /**
     * partition the graph and send the partitioned graph to the workers
     */
    public void partitionGraph(List<Integer> aliveWorkerIds) {
        if (!this.masterServer.isPrimary) {
            return;
        }
        if (this.masterServer.getGraph() == null) {
            throw new Error("graph is not set!");
        }
        nFinishWorker.set(0);
        nCompleteWorker.set(0);
        nConfirmWorker.set(0);
        superstep.set(superstepSnapshot);
        Map<Integer, VertexGroup<T>> vertexGroups = this.masterServer.getGraphPartitioner().partition(this.masterServer.getGraph(), aliveWorkerIds);
        vertexGroups.forEach((workerId, vertexGroup) -> {
            List<Mail> mails = new ArrayList<>();
            vertexGroup.getVertices().forEach(vertex -> {
                if (masterServer.getMailTable().get(vertex.getId()) != null) {
                    mails.addAll(masterServer.getMailTable().get(vertex.getId()));
                }
            });
            Map<String, Object> objectMap = new HashMap<>();
            objectMap.put("PARTITION", vertexGroup);
            objectMap.put("PARTITION_STRATEGY", this.masterServer.getGraphPartitioner().getPartitionStrategy());
            objectMap.put("MAILS", mails);
            objectMap.put("SUPERSTEP", superstep.get());
            MessageProducer.producePartitionGraphMessage(objectMap, workerId);
        });
    }

    /**
     * check whether the server is doing snapshot
     * @return whether the server is doing snapshot
     */
    public boolean isDoingSnapshot() {
        return superstep.get() % snapshotFrequency == 0;
    }
}
