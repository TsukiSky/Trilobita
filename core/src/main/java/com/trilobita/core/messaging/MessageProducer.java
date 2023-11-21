package com.trilobita.core.messaging;

import com.trilobita.commons.Computable;
import com.trilobita.commons.Mail;
import com.trilobita.commons.Message;
import com.trilobita.core.graph.Graph;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.List;
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
    private MessageProducer() {
    }

    private static final boolean LOG_FLAG = false;

    /**
     * Produce a message to a topic.
     *
     * @param key   Message {@link UUID}
     * @param value {@link Mail}, which is the message payload
     * @param topic Target topic, usually used by the destination server. It will be created if it does not exist.
     * @author Guo Ziniu : ziniu@catroll.io
     */
    public static void createAndProduce(UUID key, Mail value, int topic) {
        createAndProduce(key, value, String.valueOf(topic));
    }

    public static void createAndProduce(UUID key, Mail value, String topic) {
        try {
            MessageAdmin.getInstance().createIfNotExist(topic);
        } catch (ExecutionException | InterruptedException exception) {
            log.error("produce create topic: {}", exception.getMessage());
        }
        produce(key, value, topic);
    }

    public static void produce(UUID key, Mail value, String topic) {
        if (key == null) {
            key = UUID.randomUUID();
        }
        UUID finalKey = key;

        try (final org.apache.kafka.clients.producer.Producer<Object, Object> producer = new KafkaProducer<>(MessageAdmin.getInstance().props)) {
            producer.send(new ProducerRecord<>(topic, finalKey.toString(), value), (event, ex) -> {
                if (ex != null) {
                    log.error("[Message] error producing message: {}", ex.getMessage());
                } else {
                    if (LOG_FLAG) {
                        log.info("[Message] produced event to topic {}: key = {} value = {}", topic, finalKey, value);
                    }
                }
            });
        }
    }

    /**
     * Produce a start signal to the topic
     */
    public static void produceStartSignal(boolean doSnapshot) {
        createAndProduce(null, new Mail(-1, new Message(doSnapshot), Mail.MailType.START_SIGNAL), Mail.MailType.START_SIGNAL.ordinal());
    }

    /**
     * Produce a finish signal to the topic.
     *
     * @param vertexValues vertex values to be stored as checkpoints
     */
    public static <T> void produceFinishSignal(HashMap<Integer, Computable<T>> vertexValues) {
        Message message = new Message(vertexValues);
        Mail mail = new Mail(-1, message, Mail.MailType.FINISH_SIGNAL);
        MessageProducer.createAndProduce(null, mail, mail.getMailType().ordinal());
    }

    /**
     * Produce a sync message among masters to the topic.
     *
     * @param graph          the graph
     * @param aliveWorkerIds alive worker ids
     */
    public static void produceSyncMessage(Graph<?> graph, List<Integer> aliveWorkerIds) {
        HashMap<String, Object> syncMap = new HashMap<>();
        syncMap.put("GRAPH", graph);
        syncMap.put("ALIVE_WORKER_IDS", aliveWorkerIds);
        Message message = new Message();
        message.setContent(syncMap);
        Mail mail = new Mail();
        mail.setMessage(message);
        MessageProducer.createAndProduce(null, mail, "MASTER_SYNC");
    }
}
