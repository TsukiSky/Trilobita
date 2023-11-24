package com.trilobita.runtime.environment;

import com.trilobita.commons.Mail;
import com.trilobita.commons.Message;
import com.trilobita.core.graph.Graph;
import com.trilobita.core.messaging.MessageAdmin;
import com.trilobita.core.messaging.MessageConsumer;
import com.trilobita.core.messaging.MessageProducer;
import com.trilobita.engine.server.masterserver.partitioner.PartitionStrategy;
import com.trilobita.engine.server.masterserver.partitioner.Partitioner;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Slf4j
public class Cluster<T> {
    public final MessageConsumer clusterMessageConsumer;
    public int numOfWorkers;
    public final boolean isMaster;
    public final Integer machineId;
    public final String topic;

    private static final MessageAdmin messageAdmin = MessageAdmin.getInstance();

    public static void join(String topic) throws ExecutionException, InterruptedException {
        joinAndGet(topic);
    }

    public static <T> Cluster<T> joinAndGet(String topic) throws ExecutionException, InterruptedException {
        messageAdmin.createIfNotExist(topic);
        return new Cluster<>(topic);
    }

    public Cluster(String topic) throws ExecutionException, InterruptedException {
        this.topic = topic;
        this.machineId = messageAdmin.getConsumerGroupsForTopic(topic).size();
        log.info("machineId: {}", machineId);
        this.numOfWorkers = machineId;
        this.isMaster = machineId == 0;
        MessageProducer.createAndProduce(null, new Mail(new Message(this.machineId), Mail.MailType.NORMAL), topic);

        this.clusterMessageConsumer = new MessageConsumer(topic, machineId, new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail value, int partition, long offset) throws ExecutionException, InterruptedException {
                if (value.getMailType() == Mail.MailType.START_SIGNAL) {
                    // start the server
                    HashMap<String, Object> content = (HashMap<String, Object>) value.getMessage().getContent();
                    startServer((Graph<T>) content.get("GRAPH"), (PartitionStrategy) content.get("PARTITION_STRATEGY"));
                } else {
                    numOfWorkers += 1;
                }
            }
        });
        this.clusterMessageConsumer.start();
    }

    public void start(Graph<T> graph, PartitionStrategy partitionStrategy) {
        Mail mail = new Mail();
        mail.setMailType(Mail.MailType.START_SIGNAL);
        Map<String, Object> content = new HashMap<>();
        content.put("GRAPH", graph);
        content.put("PARTITION_STRATEGY", partitionStrategy);
        mail.setMessage(new Message(content));
        MessageProducer.createAndProduce(null, mail, topic);
    }

    public void startServer(Graph<T> graph, PartitionStrategy partitionStrategy) throws ExecutionException, InterruptedException {
        TrilobitaEnvironment<T> trilobitaEnvironment = new TrilobitaEnvironment<>();
        trilobitaEnvironment.initConfig();
        if (isMaster) {
            Thread.sleep(3000);
            trilobitaEnvironment.loadGraph(graph);
            trilobitaEnvironment.setPartitioner(new Partitioner<>(partitionStrategy));
            trilobitaEnvironment.createMasterServer(machineId, 10);
            trilobitaEnvironment.startMasterServer();
        } else {
            trilobitaEnvironment.createWorkerServer(machineId);
            trilobitaEnvironment.startWorkerServer();
        }
    }
}
