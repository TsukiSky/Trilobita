package com.trilobita.engine.server.masterserver.execution;

import com.trilobita.commons.Computable;
import com.trilobita.commons.Mail;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.core.messaging.MessageConsumer;
import com.trilobita.core.messaging.MessageProducer;
import com.trilobita.engine.server.masterserver.MasterServer;
import com.trilobita.engine.server.masterserver.execution.synchronize.Synchronizer;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ExecutionException;

@Slf4j
public class ExecutionManager<T> {
    private final MasterServer<T> masterServer;
    MessageConsumer completeSignalConsumer;
    MessageConsumer confirmStartConsumer;
    Synchronizer<T> synchronizer; // the synchronizer of the replicas
    private int nFinishWorker = 0;
    private int nCompleteWorker = 0;
    private int nConfirmWorker = 0;
    private int superstep = 0;
    private final int snapshotFrequency;

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
    }


    public void initializeConsumers() {
        // create the confirm start consumer
        this.confirmStartConsumer = new MessageConsumer("CONFIRM_RECEIVE", masterServer.getServerId(), new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail value, int partition, long offset) {
                if (!masterServer.isPrimary) {
                    return; // only the primary master can handle the confirming receive message
                }
                int senderId = (int) value.getMessage().getContent();
                log.info("[Confirm] received a confirm message from worker {}", senderId);
                nConfirmWorker += 1;
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
                if (Boolean.FALSE.equals(masterServer.isPrimary) || Boolean.TRUE.equals(masterServer.getHeartbeatManager().getIsHandlingFault())) {
                    return;
                }
                // extract the content
                Map<String, Object> content = (Map<String, Object>) value.getMessage().getContent();
                HashMap<Integer, Computable<T>> vertexValues = (HashMap<Integer, Computable<T>>) content.get("VERTEX_VALUES");
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
                }

                if (nFinishWorker == masterServer.getWorkerIds().size()) {
                    // check if the master needs to do a snapshot
                    if (isDoingSnapshot()) {
                        synchronizer.snapshotAndSync(masterServer.getGraph());
                    }
                    // check whether all workers have finished
                    if (nCompleteWorker == masterServer.getWorkerIds().size()) {
                        log.info("[Complete] the work has complete, the final graph is: {}", masterServer.getGraph());
                    } else {
                        // start a new superstep
                        Thread.sleep(300);
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
        if (this.masterServer.getGraph() == null) {
            throw new Error("graph is not set!");
        }
        nFinishWorker = 0;
        nCompleteWorker = 0;
        nConfirmWorker = 0;
        log.info("alive worker ids: {}", aliveWorkerIds);
        Map<Integer, VertexGroup<T>> vertexGroups = this.masterServer.getGraphPartitioner().partition(this.masterServer.getGraph(), aliveWorkerIds);
        vertexGroups.forEach((workerId, vertexGroup) -> {
            Map<String, Object> objectMap = new HashMap<>();
            objectMap.put("PARTITION", vertexGroup);
            objectMap.put("PARTITION_STRATEGY", this.masterServer.getGraphPartitioner().getPartitionStrategy());
            MessageProducer.producePartitionGraphMessage(objectMap, workerId);
        });
    }

    /**
     * check whether the server is doing snapshot
     * @return whether the server is doing snapshot
     */
    public boolean isDoingSnapshot() {
        return superstep % snapshotFrequency == 0;
    }
}
