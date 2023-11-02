package com.trilobita.core.messaging;

import com.trilobita.commons.Mail;
import com.trilobita.commons.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
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
    private static final boolean willLog = false;

    /**
     * Produce a message to a topic.
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
                    log.error("error producing message: {}", ex.getMessage());
                } else {
                    if (willLog){
                        log.info("Produced event to topic {}: key = {} value = {}", topic, finalKey, value);
                    }
                }
            });
        }
    }

    /**
     * Produce a start signal to the topic
     */
    public static void produceStartSignal() {
        createAndProduce(null, new Mail(-1, null, Mail.MailType.START_SIGNAL), Mail.MailType.START_SIGNAL.ordinal());
    }

    /**
     * Produce a finish signal to the topic.
     * @param superstep current superstep
     * @param vertexValues vertex values to be stored as checkpoints
     */
    public static void produceFinishSignal(int superstep, HashMap<Integer, Object> vertexValues) {
        Message message = new Message(new HashMap<>(vertexValues));
        Mail mail = new Mail(-1, message, Mail.MailType.FINISH_SIGNAL);
        log.info("super step {} finished", superstep);
        MessageProducer.createAndProduce(null, mail, mail.getMailType().ordinal());
    }
}
