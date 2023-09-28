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

    MessageAdmin messageAdmin = MessageAdmin.getInstance();

    /**
     * <p>Produce a message to a topic.
     * </p>
     *
     * @param key   Message {@link UUID}
     * @param value {@link Mail}, which is the message payload
     * @param topic Target topic, usually used by the destination server. It will be created if it does not exist.
     * @author Guo Ziniu : ziniu@catroll.io
     */
    public void produce(UUID key, Mail value, String topic) throws ExecutionException, InterruptedException {
        if (key == null) {
            key = UUID.randomUUID();
        }
        UUID finalKey = key;
        messageAdmin.createIfNotExist(topic);
        try (final org.apache.kafka.clients.producer.Producer<Object, Object> producer = new KafkaProducer<>(messageAdmin.props)) {
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