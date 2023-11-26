package com.trilobita.engine.server.masterserver.heartbeat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trilobita.commons.Mail;
import com.trilobita.core.messaging.MessageConsumer;
import com.trilobita.engine.server.masterserver.MasterServer;
import com.trilobita.engine.server.masterserver.heartbeat.checker.HeartbeatChecker;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * HeartbeatManager is used to manage the heartbeat of the workers and masters
 * It contains the fault handler for the workers and masters
 */
@Slf4j
public class HeartbeatManager {
    MasterServer<?> masterServer;
    List<Integer> workerIds;
    List<Integer> masterIds;
    MessageConsumer workerHeatBeatConsumer;
    HeartbeatChecker masterHeartbeatChecker;
    MessageConsumer masterHeatBeatConsumer;
    HeartbeatChecker workerHeartbeatChecker;
    MessageConsumer stopSignalConsumer;

    @Getter
    Boolean isHandlingFault = false;

    public HeartbeatManager(MasterServer<?> masterServer, List<Integer> workerIds, List<Integer> masterIds) {
        this.masterServer = masterServer;
        this.workerIds = workerIds;
        this.masterIds = masterIds;
        initializeHeartbeatChecker();
        initializeHeartbeatConsumer();
    }

    /**
     * Initialize the heartbeat checker for workers and masters
     */
    public void initializeHeartbeatChecker() {
        // create heartbeat checker for workers
        workerHeartbeatChecker = new HeartbeatChecker(masterServer.getServerId(), workerIds, true, new HeartbeatChecker.FaultHandler() {
            @Override
            public void handleFault(List<Integer> errors) {
                isHandlingFault = true;
                for (Integer id : errors){
                    log.info("[Fault] server {} is down, start repartitioning...", id);
                    workerIds.remove(id);
                    workerHeartbeatChecker.getHeartbeats().remove(id);
                }
                masterServer.getExecutionManager().partitionGraph(workerIds);
                log.info("finished repartitioning...");
                isHandlingFault = false;
            }
        });
        masterHeartbeatChecker = new HeartbeatChecker(masterServer.getServerId(), masterIds, false, new HeartbeatChecker.FaultHandler() {
            @Override
            public void handleFault(List<Integer> errors) {
                if (masterServer.isPrimary) {
                    return;
                }
                // if all id with greater id has died, become the master
                log.info("[Fault] detected current master is down, trying to become master...");
                masterServer.isPrimary = true;
                masterServer.start();
                masterServer.getExecutionManager().partitionGraph(workerIds);
                isHandlingFault = false;
            }
        });
    }

    /**
     * Initialize the heartbeat consumer for workers and masters
     */
    public void initializeHeartbeatConsumer() {
        // create heartbeat checker for workers
        workerHeatBeatConsumer = new MessageConsumer("HEARTBEAT_WORKER", masterServer.getServerId(), new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail value, int partition, long offset) {
                int senderId = (int) value.getMessage().getContent();
                if (!workerIds.contains(senderId)) {
                    // new worker joins, repartition the graph
                    workerIds.add(senderId);
                    workerHeartbeatChecker.getHeartbeats().put(senderId, true);
                    masterServer.getExecutionManager().partitionGraph(workerIds);
                } else {
                    // update the heartbeat
                    workerHeartbeatChecker.recordHeartbeatFrom(senderId);
                }
            }
        });
        // create heartbeat checker for master and replicas
        masterHeatBeatConsumer = new MessageConsumer("HEARTBEAT_MASTER", masterServer.getServerId(), new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail value, int partition, long offset) {
                int senderId = (int) value.getMessage().getContent();
                if (senderId > masterServer.getServerId()) {
                    masterServer.isPrimary = false; // the server with a larger id is the primary master server
                }
                masterHeartbeatChecker.recordHeartbeatFrom(senderId);
            }
        });
        this.stopSignalConsumer = new MessageConsumer("STOP", this.masterServer.getServerId(), new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail value, int partition, long offset) throws JsonProcessingException, InterruptedException, ExecutionException {
                masterServer.shutdown();
            }
        });
    }

    /**
     * Start monitoring the heartbeat of the workers and masters
     */
    public void listen() throws ExecutionException, InterruptedException {
        startMonitorWorkerHeartbeat();
        startMonitorMasterAndReplicaHeartbeat();
        stopSignalConsumer.start();
    }

    public void stop() throws InterruptedException {
        stopMonitorWorkerHeartbeat();
        stopMonitorMasterAndReplicaHeartbeat();
        stopSignalConsumer.stop();
    }

    /**
     * Start monitoring the heartbeat of the workers
     */
    public void startMonitorWorkerHeartbeat() throws ExecutionException, InterruptedException {
        this.workerHeartbeatChecker.start();
        this.workerHeatBeatConsumer.start();
    }

    public void stopMonitorWorkerHeartbeat() throws InterruptedException {
        this.workerHeartbeatChecker.stop();
        this.workerHeatBeatConsumer.stop();
    }

    /**
     * Start monitoring the heartbeat of the master and replicas
     */
    public void startMonitorMasterAndReplicaHeartbeat() throws ExecutionException, InterruptedException {
        this.masterHeartbeatChecker.start();
        this.masterHeatBeatConsumer.start();
    }

    public void stopMonitorMasterAndReplicaHeartbeat() throws InterruptedException {
        this.masterHeartbeatChecker.stop();
        this.masterHeatBeatConsumer.stop();
    }

    public void removeWorker(int id) {
        workerIds.remove(id);
        workerHeartbeatChecker.getHeartbeats().remove(id);
    }

    public void addWorker(int id) {
        workerIds.add(id);
        workerHeartbeatChecker.getHeartbeats().put(id, true);
    }

    public void removeMaster(int id) {
        masterIds.remove(id);
        masterHeartbeatChecker.getHeartbeats().remove(id);
    }

    public void addMaster(int id) {
        masterIds.add(id);
        masterHeartbeatChecker.getHeartbeats().put(id, true);
    }
}
