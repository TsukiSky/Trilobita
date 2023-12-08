package com.trilobita.engine.server.masterserver.heartbeat;

import com.trilobita.core.common.Mail;
import com.trilobita.core.messaging.MessageConsumer;
import com.trilobita.engine.server.masterserver.MasterServer;
import com.trilobita.engine.server.masterserver.heartbeat.checker.HeartbeatChecker;
import com.trilobita.engine.server.util.HeartbeatSender;
import lombok.Data;
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
@Data
public class HeartbeatManager {
    MasterServer<?> masterServer;
    List<Integer> masterIds;
    MessageConsumer workerHeartBeatConsumer;

    HeartbeatChecker masterHeartBeatChecker;
    MessageConsumer masterHeartBeatConsumer;
    HeartbeatChecker workerHeartBeatChecker;
    HeartbeatSender heartbeatSender;
    MessageConsumer stopSignalConsumer;

    @Getter
    volatile Boolean isHandlingFault = false;

    public HeartbeatManager(MasterServer<?> masterServer, List<Integer> masterIds) {
        this.masterServer = masterServer;
        this.masterIds = masterIds;
        initializeHeartbeatSender();
        initializeHeartbeatChecker();
        initializeHeartbeatConsumer();
    }

    public void initializeHeartbeatSender() {
        this.heartbeatSender = new HeartbeatSender(this.masterServer.getServerId(), false);
    }

    /**
     * Initialize the heartbeat checker for workers and masters
     */
    public void initializeHeartbeatChecker() {
        // create heartbeat checker for workers
        workerHeartBeatChecker = new HeartbeatChecker(masterServer.getServerId(), this.masterServer.getWorkerIds(), true, new HeartbeatChecker.FaultHandler() {
            @Override
            public void handleFault(List<Integer> errors) {
                isHandlingFault = true;
                for (Integer id : errors){
                    log.info("[Fault] server {} is down", id);
                    masterServer.getWorkerIds().remove((Integer) id);
                    workerHeartBeatChecker.getHeartbeats().remove(id);
                }
                log.info("worker ids: {}", masterServer.getWorkerIds());
                masterServer.getExecutionManager().partitionGraph(masterServer.getWorkerIds());
            }
        });
        masterHeartBeatChecker = new HeartbeatChecker(masterServer.getServerId(), masterIds, false, new HeartbeatChecker.FaultHandler() {
            @Override
            public void handleFault(List<Integer> errors) {
                if (masterServer.isPrimary) {
                    return;
                }
                // if all id with greater id has died, become the master
                log.info("[Fault] detected current master is down, trying to become master...");
                masterServer.isPrimary = true;
                masterServer.getExecutionManager().partitionGraph(masterServer.getWorkerIds());
                masterServer.getMasterFunctionableRunner().becomePrimary();
                isHandlingFault = false;
            }
        });
    }

    /**
     * Initialize the heartbeat consumer for workers and masters
     */
    public void initializeHeartbeatConsumer() {
        // create heartbeat checker for workers
        workerHeartBeatConsumer = new MessageConsumer("HEARTBEAT_WORKER", masterServer.getServerId(), new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail value, int partition, long offset) {
                if (isHandlingFault) {
                    return;
                }
                int senderId = (int) value.getMessage().getContent();
                if (!masterServer.getWorkerIds().contains(senderId)) {
                    System.out.println(masterServer.getWorkerIds());
                    masterServer.getWorkerIds().add(senderId);
                    workerHeartBeatChecker.getHeartbeats().put(senderId, true);
                    masterServer.getExecutionManager().partitionGraph(masterServer.getWorkerIds());
                    masterServer.getMasterFunctionableRunner().broadcastFunctionables();
                } else {
                    // update the heartbeat
                    workerHeartBeatChecker.recordHeartbeatFrom(senderId);
                }
            }
        });
        // create heartbeat checker for master and replicas
        masterHeartBeatConsumer = new MessageConsumer("HEARTBEAT_MASTER", masterServer.getServerId(), new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail value, int partition, long offset) {
                int senderId = (int) value.getMessage().getContent();
                if (senderId > masterServer.getServerId()) {
                    masterServer.isPrimary = false; // the server with a larger id is the primary master server
                }
                masterHeartBeatChecker.recordHeartbeatFrom(senderId);
            }
        });
        this.stopSignalConsumer = new MessageConsumer("STOP", this.masterServer.getServerId(), new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail value, int partition, long offset) throws InterruptedException, ExecutionException {
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
        this.heartbeatSender.start();
    }

    public void stop() throws InterruptedException {
        stopMonitorWorkerHeartbeat();
        stopMonitorMasterAndReplicaHeartbeat();
        stopSignalConsumer.stop();
        this.heartbeatSender.stop();
    }

    /**
     * Start monitoring the heartbeat of the workers
     */
    public void startMonitorWorkerHeartbeat() throws ExecutionException, InterruptedException {
        this.workerHeartBeatChecker.start();
        this.workerHeartBeatConsumer.start();
    }

    public void stopMonitorWorkerHeartbeat() throws InterruptedException {
        this.workerHeartBeatChecker.stop();
        this.workerHeartBeatConsumer.stop();
    }

    /**
     * Start monitoring the heartbeat of the master and replicas
     */
    public void startMonitorMasterAndReplicaHeartbeat() throws ExecutionException, InterruptedException {
        this.masterHeartBeatChecker.start();
        this.masterHeartBeatConsumer.start();
    }

    public void stopMonitorMasterAndReplicaHeartbeat() throws InterruptedException {
        this.masterHeartBeatChecker.stop();
        this.masterHeartBeatConsumer.stop();
    }

    public void removeWorker(int id) {
        this.masterServer.getWorkerIds().remove(id);
        workerHeartBeatChecker.getHeartbeats().remove(id);
    }

    public void addWorker(int id) {
        this.masterServer.getWorkerIds().add(id);
        workerHeartBeatChecker.getHeartbeats().put(id, true);
    }

    public void removeMaster(int id) {
        masterIds.remove(id);
        masterHeartBeatChecker.getHeartbeats().remove(id);
    }

    public void addMaster(int id) {
        masterIds.add(id);
        masterHeartBeatChecker.getHeartbeats().put(id, true);
    }
}
