package com.trilobita.core.messaging;

import com.trilobita.commons.Mail;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

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

    /**
     * <p>Produce a message to a topic.
     * </p>
     *
     * @param key   Message {@link UUID}
     * @param value {@link Mail}, which is the message payload
     * @param topic Target topic, usually used by the destination server. It will be created if it does not exist.
     * @author Guo Ziniu : ziniu@catroll.io
     */
    public static void produce(UUID key, Mail value, String topic) {
        if (key == null) {
            key = UUID.randomUUID();
        }
        UUID finalKey = key;
        try {
            MessageAdmin.getInstance().createIfNotExist(topic);
        } catch (ExecutionException | InterruptedException exception) {
log.error("produce create topic: {}", exception.getMessage());
        }

        try (final org.apache.kafka.clients.producer.Producer<Object, Object> producer = new KafkaProducer<>(MessageAdmin.getInstance().props)) {
            producer.send(new ProducerRecord<>(topic, finalKey, value), (event, ex) -> {
                if (ex != null) {
log.error("error producing message: {}", ex.getMessage());
                } else {
log.info("Produced event to topic {}: key = {} value = {}", topic, finalKey, value);
                }
            });
        }
    }

}