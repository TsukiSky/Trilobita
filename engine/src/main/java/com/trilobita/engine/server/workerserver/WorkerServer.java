package com.trilobita.engine.server.workerserver;

import com.trilobita.core.common.Mail;
import com.trilobita.core.common.Message;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.core.messaging.MessageConsumer;
import com.trilobita.core.messaging.MessageProducer;
import com.trilobita.engine.monitor.Monitor;
import com.trilobita.engine.monitor.metrics.Metrics;
import com.trilobita.engine.server.AbstractServer;
import com.trilobita.engine.server.util.HeartbeatSender;
import com.trilobita.engine.server.masterserver.partition.strategy.PartitionStrategy;
import com.trilobita.engine.server.util.functionable.functionableRunner.WorkerFunctionableRunner;
import com.trilobita.engine.server.workerserver.execution.ExecutionManager;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;

/**
 * Worker Server is the worker of a server cluster.
 * It executes the superstep, controls vertex computations and communicate with other servers
 *
 * @param <T> the type of the vertex value
 */
@Slf4j
@Getter
public class WorkerServer<T> extends AbstractServer<T> {
    private final ExecutionManager<T> executionManager;
    private final ConcurrentHashMap<Integer, CopyOnWriteArrayList<Mail>> outMailTable;
    @Setter
    private List<Mail> snapshotMails = new ArrayList<>();
    private final MessageConsumer partitionMessageConsumer;
    private final MessageConsumer startMessageConsumer;
    private final HeartbeatSender heartbeatSender;
    private final MessageConsumer confirmStartConsumer;
    private final MessageConsumer stopSignalConsumer;
    private final WorkerFunctionableRunner functionableRunner;
    private int superstep = 0;

    public WorkerServer(int serverId, int parallelism, PartitionStrategy partitionStrategy) throws ExecutionException, InterruptedException {
        super(serverId, partitionStrategy);
        this.executionManager = new ExecutionManager<>(parallelism, this);
        this.outMailTable = new ConcurrentHashMap<>();
        this.confirmStartConsumer = new MessageConsumer("CONFIRM_START", this.getServerId(), new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail value, int partition, long offset) throws InterruptedException {
                if (getVertexGroup() != null) {
                    WorkerServer.this.superstep(false);
                }
            }
        });
        this.partitionMessageConsumer = new MessageConsumer("SERVER_" + this.getServerId() + "_PARTITION", serverId, new MessageConsumer.MessageHandler() {
            @Override
            @SuppressWarnings("unchecked")
            public void handleMessage(UUID key, Mail mail, int partition, long offset) throws InterruptedException, ExecutionException {
                log.info("[Signal] Receive VertexGroup from master");
                Monitor.start();
                Metrics.setWorkerStartTime();
                WorkerServer.this.executionManager.waitForFutures(); // in case of fault, repartition is needed
                Map<String, Object> res = (Map<String, Object>) mail.getMessage().getContent();
                setVertexGroup((VertexGroup<T>) res.get("PARTITION"));
                PartitionStrategy partitionStrategy = (PartitionStrategy) res.get("PARTITION_STRATEGY");
                List<Mail> incomingMails = (List<Mail>) res.get("MAILS");
                if (!incomingMails.isEmpty()) {
                    getInMailQueue().addAll(incomingMails);
                }
                setPartitionStrategy(partitionStrategy);
                // assign the server's hashmap to each vertex
                Metrics.Superstep.initialize();
                List<Vertex<T>> vertices = vertexGroup.getVertices();
                for (Vertex<T> vertex : vertices) {
                    Metrics.Superstep.incrementVertexNum(1);
                    Metrics.Superstep.incrementEdgeNum(vertex.getEdges().size());
                    vertex.setServerQueue(getOutMailQueue());
                }
                Message message = new Message();
                message.setContent(WorkerServer.this.getServerId());
                Mail mailToConfirmReceive = new Mail();
                mailToConfirmReceive.setMessage(message);
                MessageProducer.produce(null, mailToConfirmReceive,"CONFIRM_RECEIVE");
            }
        });

        this.startMessageConsumer = new MessageConsumer(Mail.MailType.START_SIGNAL.ordinal(), serverId, new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail mail, int partition, long offset) throws InterruptedException {
                if (getVertexGroup() != null) {
//                    log.info("[Signal] Receive <Superstep Start> from master");
                    boolean doSnapshot = (boolean) mail.getMessage().getContent();
                    superstep(doSnapshot);
                }
            }
        });
        this.stopSignalConsumer = new MessageConsumer("STOP", this.getServerId(), new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail value, int partition, long offset) throws InterruptedException {
                log.info("[Signal] Receive <Stop> from master");
                shutdown();
            }
        });
        this.heartbeatSender = new HeartbeatSender(this.getServerId(), true);
        this.functionableRunner = WorkerFunctionableRunner.getInstance(serverId);
        log.info("[WorkerServer] Worker {} initialized with a parallelism of {}", serverId, parallelism);
    }

    /**
     * Start running the server
     */
    @Override
    public void start() throws InterruptedException, ExecutionException {
        setServerStatus(ServerStatus.RUNNING);
        startMessageConsumer.start();
        partitionMessageConsumer.start();
        confirmStartConsumer.start();
        getMessageConsumer().start();
        heartbeatSender.start();
        stopSignalConsumer.start();
    }

    /**
     * Execute the superstep
     */
    private void superstep(boolean doSnapshot) throws InterruptedException {
        superstep++;
        log.info("[Superstep] Start Superstep {}", superstep);
        Metrics.Superstep.setSuperstepStartTime();
        this.executionManager.setDoSnapshot(doSnapshot);
        this.executionManager.execute();

        boolean stop = true;
        for (Vertex<T> v: this.vertexGroup.getVertices()){
            if (!v.isShouldStop() && v.getStatus() == Vertex.VertexStatus.ACTIVE) {
                stop = false;
                break;
            }
        }
        sendCompleteSignal(doSnapshot, stop);
        Metrics.Superstep.computeSuperstepDuration();
        Monitor.stopAndStartNewSuperstep();
    }

    @Override
    public void pause() {
        setServerStatus(ServerStatus.PAUSE);
    }

    @Override
    public void shutdown() throws InterruptedException {
        Monitor.stop();
        Metrics.computeWorkerDuration();
        Monitor.store("data/performance/worker"+this.getServerId());
        startMessageConsumer.stop();
        partitionMessageConsumer.stop();
        confirmStartConsumer.stop();
        getMessageConsumer().stop();
        heartbeatSender.stop();
        stopSignalConsumer.stop();
        executionManager.stop();
        functionableRunner.stop();
    }

    /**
     * Send a complete signal to the master server
     */
    public void sendCompleteSignal(boolean doSnapshot, boolean complete) {
        log.info("[Superstep] Superstep {} completed", superstep);
        log.info("#############################################################################################");
        if (doSnapshot) {
            MessageProducer.produceFinishSignal(this.vertexGroup.getVertexValues(), this.snapshotMails, complete);
        } else {
            MessageProducer.produceFinishSignal(new HashMap<>(), new ArrayList<>(), false);
        }
    }

    /**
     * Distribute mail to vertex
     * @param mail mail to be distributed
     */
    public void distributeMailToVertex(Mail mail) {
        Vertex<T> vertex = findVertexById(mail.getToVertexId());
        if (vertex != null) {
            vertex.onReceive(mail);
        }
    }

    /**
     * Find the vertex with the given id
     *
     * @param vertexId vertex id
     * @return vertex with the given id
     */
    private Vertex<T> findVertexById(int vertexId) {
        return this.getVertexGroup().getVertexById(vertexId);
    }
}
