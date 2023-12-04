package com.trilobita.engine.server.masterserver.execution;

import com.trilobita.commons.Computable;
import com.trilobita.commons.Mail;
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

@Slf4j
public class ExecutionManager<T> {
    public final Map<Integer, List<Mail>> snapshotMailTable = new HashMap<>();
    private final MasterServer<T> masterServer;
    private final int snapshotFrequency;
    MessageConsumer completeSignalConsumer;
    MessageConsumer confirmStartConsumer;
    Synchronizer<T> synchronizer; // the synchronizer of the replicas
    private int nFinishWorker = 0;
    private int nCompleteWorker = 0;
    private int nConfirmWorker = 0;
    @Getter
    private int superstep = 0;

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
                log.info("[Confirm] received a confirm message from worker {}", senderId);
                nConfirmWorker += 1;
                log.info("nconfirmworker: {}, alive workers: {}", nConfirmWorker, masterServer.getWorkerIds());
                if (nConfirmWorker == masterServer.getWorkerIds().size()) {
                    // send start message to all workers
                    MessageProducer.createAndProduce(null, new Mail(), "CONFIRM_START");
                }
            }
        });

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
                if (complete) {
                    nCompleteWorker++;
                }
                nFinishWorker++;
                log.info("[Superstep] number of finished workers: {}", nFinishWorker);

                if (!vertexValues.isEmpty()) {
                    // update the graph
                    masterServer.getGraph().updateVertexValues(vertexValues);
                    log.info("[Graph] the updated graph is : {}", masterServer.getGraph());
                    for (Mail snapshotMail : snapshotMails) {
                        snapshotMailTable.computeIfAbsent(snapshotMail.getToVertexId(), k -> new ArrayList<>());
                        snapshotMailTable.get(snapshotMail.getToVertexId()).add(snapshotMail);
                    }
                }

                if (nFinishWorker == masterServer.getWorkerIds().size()) {
                    // aggregate functional values and send to workers
                    masterServer.getMasterFunctionableRunner().runFunctionableTasks();
                    log.info("[Functionable] finished executing Functionable tasks");

                    Metrics.Superstep.computeMasterDuration();
                    Monitor.stopAndStartNewSuperstepMaster();
                    // check if the master needs to do a snapshot
                    if (isDoingSnapshot()) {
                        synchronizer.snapshotAndSync(masterServer.getGraph());
                    }
                    // check whether all workers have finished
                    if (nCompleteWorker == masterServer.getWorkerIds().size()) {
                        log.info("[Complete] the work has complete, the final graph is: {}", masterServer.getGraph());
                        masterServer.shutdown();
                    } else {
                        // start a new superstep
                        Thread.sleep(300);
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
        this.superstep += 1;
        nFinishWorker = 0;
        nCompleteWorker = 0;
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
        nFinishWorker = 0;
        nCompleteWorker = 0;
        nConfirmWorker = 0;
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
            MessageProducer.producePartitionGraphMessage(objectMap, workerId);
        });
    }

    /**
     * check whether the server is doing snapshot
     *
     * @return whether the server is doing snapshot
     */
    public boolean isDoingSnapshot() {
        return superstep % snapshotFrequency == 0;
    }
}
