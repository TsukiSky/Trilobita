package com.trilobita.runtime.environment;

import com.trilobita.core.common.Mail;
import com.trilobita.core.common.Message;
import com.trilobita.core.graph.Graph;
import com.trilobita.core.messaging.MessageAdmin;
import com.trilobita.core.messaging.MessageConsumer;
import com.trilobita.core.messaging.MessageProducer;
import com.trilobita.engine.server.masterserver.partition.strategy.PartitionStrategy;
import com.trilobita.engine.server.masterserver.partition.Partitioner;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * TrilobitaCluster is the API for users to submit a graph to the cluster, and start the cluster.
 * @param <T> the type of the vertex value
 */
@Slf4j
public class TrilobitaCluster<T> {
    public final MessageConsumer clusterConsumer;   // consumer for cluster messages
    public int numOfWorkers;    // number of workers in the cluster
    public final boolean isMaster;  // flag indicates whether this machine is the master
    public final Integer machineId; // id of this machine
    public final String topic;  // topic of the cluster
    public final TrilobitaEnvironment<T> trilobitaEnvironment = new TrilobitaEnvironment<>();   // TrilobitaEnvironment of this machine
    private static final MessageAdmin messageAdmin = MessageAdmin.getInstance();

    public static void join(String topic) throws ExecutionException, InterruptedException {
        joinAndGet(topic);
    }

    public static <T> TrilobitaCluster<T> joinAndGet(String topic) throws ExecutionException, InterruptedException {
        messageAdmin.createIfNotExist(topic);
        return new TrilobitaCluster<>(topic);
    }

    public TrilobitaCluster(String topic) throws ExecutionException, InterruptedException {
        this.topic = topic;
        this.machineId = messageAdmin.getConsumerGroupsForTopic(topic).size();
        log.info("[JOIN CLUSTER] Machine joins the cluster --> MachineId: {}", machineId);
        this.numOfWorkers = machineId;  // the current number of workers is the current number of machines in the cluster
        this.isMaster = machineId == 0; // the first machine is the master
        MessageProducer.createAndProduce(null, new Mail(new Message(this.machineId), Mail.MailType.NORMAL), topic);

        this.clusterConsumer = new MessageConsumer(topic, machineId, new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail value, int partition, long offset) throws ExecutionException, InterruptedException {
                switch (value.getMailType()) {
                    case SUBMIT_JOB:
                        // A job is submitted to the cluster
                        if (!isMaster) {
                            return; // only the master needs to handle this message
                        }
                        log.info("[SUBMIT JOB] a job is submitted to the cluster");
                        HashMap<String, Object> content = (HashMap<String, Object>) value.getMessage().getContent();
                        Graph<T> graph = (Graph<T>) content.get("GRAPH");
                        PartitionStrategy partitionStrategy = (PartitionStrategy) content.get("PARTITION_STRATEGY");
                        trilobitaEnvironment.loadGraph(graph);
                        trilobitaEnvironment.setPartitioner(new Partitioner<>(partitionStrategy));
                        trilobitaEnvironment.createMasterServer(machineId, 10, false);
                    case START_SIGNAL:
                        log.info("[START SIGNAL] Starting the cluster");
                        break;
                    case FINISH_SIGNAL:
                        log.info("[FINISH SIGNAL] Finishing the cluster");
                        break;
                    case HEARTBEAT:
                        log.info("[HEARTBEAT] Heartbeat from the cluster");
                        break;
                    case PARTITION:
                        log.info("[PARTITION] Partitioning the graph");
                        break;
                    case FUNCTIONAL:
                        log.info("[FUNCTIONAL] Functional message from the cluster");
                        break;
                    case NORMAL:
                        log.info("[NORMAL] Normal message from the cluster");
                        break;
                    default:
                        log.info("[UNKNOWN] Unknown message from the cluster");
                        break;
                }

                if (value.getMailType() == Mail.MailType.START_SIGNAL) {
                    // start the server
                    HashMap<String, Object> content = (HashMap<String, Object>) value.getMessage().getContent();
                    startServer((Graph<T>) content.get("GRAPH"), (PartitionStrategy) content.get("PARTITION_STRATEGY"));
                } else {
                    numOfWorkers += 1;
                }
            }
        });
        this.clusterConsumer.start();
    }

    /**
     * initialize the server
     * @param isMaster whether this machine is the master
     */
    private void initializeServer(boolean isMaster) throws ExecutionException, InterruptedException {
        trilobitaEnvironment.initConfig();
        if (isMaster) {
            // initialize the master server
            trilobitaEnvironment.createMasterServer(machineId, 10, false);
        } else {
            // initialize the worker server
            trilobitaEnvironment.createWorkerServer(machineId);
        }
    }

    /**
     * submit the graph to the cluster
     * @param graph the graph to be submitted
     * @param partitionStrategy the partition strategy
     */
    public void submit(Graph<T> graph, PartitionStrategy partitionStrategy) {
        Mail submitMail = new Mail();
        submitMail.setMailType(Mail.MailType.SUBMIT_JOB);
        Map<String, Object> content = new HashMap<>();
        content.put("GRAPH", graph);
        content.put("PARTITION_STRATEGY", partitionStrategy);
        submitMail.setMessage(new Message(content));
        MessageProducer.createAndProduce(null, submitMail, topic);
    }

    private void startServer(Graph<T> graph, PartitionStrategy partitionStrategy) throws ExecutionException, InterruptedException {
        TrilobitaEnvironment<T> trilobitaEnvironment = new TrilobitaEnvironment<>();
        trilobitaEnvironment.initConfig();
        if (isMaster) {
            Thread.sleep(3000);
            trilobitaEnvironment.loadGraph(graph);
            trilobitaEnvironment.setPartitioner(new Partitioner<>(partitionStrategy));
            trilobitaEnvironment.createMasterServer(machineId, 10, false);
            trilobitaEnvironment.startMasterServer();
        } else {
            trilobitaEnvironment.createWorkerServer(machineId);
            trilobitaEnvironment.startWorkerServer();
        }
    }
}
