package com.trilobita.core.messaging;

import com.trilobita.core.common.Computable;
import com.trilobita.core.common.Mail;
import com.trilobita.core.common.Message;
import com.trilobita.core.common.Snapshot;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * <h1>MessageProducer</h1>
 * Produce messages to a certain Kafka topic.
 *
 * @author Guo Ziniu: ziniu@catroll.io
 */
@Slf4j
public class MessageProducer {
    static Producer<Object, Object> producer = new KafkaProducer<>(MessageAdmin.getInstance().props);

    private MessageProducer() {
        producer = new KafkaProducer<>(MessageAdmin.getInstance().props);
    }


    private static final boolean LOG_FLAG = false;

    /**
     * Produce a message to a topic.
     *
     * @param key   Message {@link UUID}
     * @param value {@link Mail}, which is the message payload
     * @param topic Target topic, usually used by the destination server. It will be
     *              created if it does not exist.
     * @author Guo Ziniu : ziniu@catroll.io
     */
    public static void produce(UUID key, Mail value, int topic) {
        produce(key, value, String.valueOf(topic));
    }

    public static void produce(UUID key, Mail value, String topic) {
        doProduce(key, value, topic);
    }

    public static void createTopic(String topic) {
        try {
            MessageAdmin.getInstance().createIfNotExist(topic);
        } catch (ExecutionException | InterruptedException exception) {
            log.error("produce create topic: {}", exception.getMessage());
        }
    }

    public static void createAndProduce(UUID key, Mail value, String topic) {
        try {
            MessageAdmin.getInstance().createIfNotExist(topic);
        } catch (ExecutionException | InterruptedException exception) {
            log.error("produce create topic: {}", exception.getMessage());
        }
        doProduce(key, value, topic);
    }

    public static void doProduce(UUID key, Mail value, String topic) {
        if (key == null) {
            key = UUID.randomUUID();
        }
        UUID finalKey = key;

        producer.send(new ProducerRecord<>(topic, finalKey.toString(), value), (event, ex) -> {
            if (ex != null) {
            } else {
                if (LOG_FLAG) {
                }
            }
        });
    }

    /**
     * Produce a start signal to the topic
     */
    public static void produceStartSignal(boolean doSnapshot) {
        produce(null, new Mail(-1, new Message(doSnapshot), Mail.MailType.START_SIGNAL),
                Mail.MailType.START_SIGNAL.ordinal());
    }

    /**
     * Produce a finish signal to the topic.
     *
     * @param vertexValues vertex values to be stored as checkpoints
     */
    public static <T> void produceFinishSignal(HashMap<Integer, Computable<T>> vertexValues, List<Mail> snapshotMails, boolean complete, int superstep) {
        Map<String, Object> map = new HashMap<>();
        map.put("VERTEX_VALUES", vertexValues);
        map.put("COMPLETE", complete);
        map.put("SNAPSHOT_MAILS", snapshotMails);
        map.put("SUPERSTEP", superstep);
        Message message = new Message(map);
        Mail mail = new Mail(-1, message, Mail.MailType.FINISH_SIGNAL);
        MessageProducer.produce(null, mail, mail.getMailType().ordinal());
    }

    /**
     * Produce a snapshot message to the topic.
     *
     * @param snapshot snapshot to be sent
     */
    public static void produceSyncMessage(Snapshot<?> snapshot) {
        HashMap<String, Object> syncContent = new HashMap<>();
        syncContent.put("SNAPSHOT", snapshot);
        Message message = new Message();
        message.setContent(syncContent);
        Mail mail = new Mail();
        mail.setMessage(message);
        MessageProducer.produce(null, mail, "MASTER_SYNC");
    }

    /**
     * Produce a heartbeat message to the topic.
     *
     * @param serverId server id
     * @param isWorker whether the server is worker or not
     */
    public static void produceHeartbeatMessage(int serverId, boolean isWorker) {
        Message message = new Message(serverId);
        Mail mail = new Mail(message, Mail.MailType.HEARTBEAT);
        String topic = isWorker ? "HEARTBEAT_WORKER" : "HEARTBEAT_MASTER";
        MessageProducer.produce(null, mail, topic);
    }

    /**
     * Produce a graph partition message to a worker to the topic.
     *
     * @param objectMap the content
     * @param serverId  worker id
     */
    public static void producePartitionGraphMessage(Object objectMap, Integer serverId) {
        Message message = new Message(objectMap);
        Mail mail = new Mail(-1, message, Mail.MailType.PARTITION);
        MessageProducer.produce(null, mail, "SERVER_" + serverId + "_PARTITION");
    }

    /**
     * Produce a normal message to a worker to the topic.
     *
     * @param mail     the mail to be sent
     * @param serverId receiver worker id
     */
    public static void produceWorkerServerMessage(Mail mail, Integer serverId) {
        MessageProducer.produce(null, mail, "SERVER_" + serverId + "_MESSAGE");
    }
}
