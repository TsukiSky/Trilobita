package com.trilobita.core.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trilobita.commons.Mail;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;

/**
 * <h1>MessageConsumer</h1>
 * Consume messages from a certain Kafka topic.
 *
 * @author Guo Ziniu: ziniu@catroll.io
 */
@Slf4j
public class MessageConsumer {
    private volatile boolean runFlag = false;
    private String topic;
    private final MessageAdmin messageAdmin = MessageAdmin.getInstance();
    private final Properties consumerProperties = new Properties();
    private Thread consumerThread;
    private final MessageHandler messageHandler;

    public interface MessageHandler {
        /**
         * <p>Handle message received from a topic.
         * <b>Note: </b>This method should not handle complicated tasks as it will block the consumer(listener) thread!
         * It is recommended to add messages to a {@link BlockingQueue}.
         * </p>
         *
         * @param key       Message {@link UUID}
         * @param value     {@link Mail}, which is the message payload
         * @param partition message partition
         * @param offset    message offset
         * @author Guo Ziniu: ziniu@catroll.io
         */
        void handleMessage(UUID key, Mail value, int partition, long offset) throws JsonProcessingException, InterruptedException, ExecutionException;
    }

    public MessageConsumer(String topic, Integer serverId, MessageHandler messageHandler) {
        consumerProperties.putAll(messageAdmin.props);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-trilobita-"+ serverId);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        this.messageHandler = messageHandler;
        this.topic = topic;
    }

    /**
     * <p>Start listening to the topic in a new thread.
     * </p>
     *
     * @author Guo Ziniu: ziniu@catroll.io
     */
    public void start() throws ExecutionException, InterruptedException {
        Set<String> existing = messageAdmin.getTopics();
        if (!existing.contains(topic)) {
            log.info("existing topic: {} do not contain {}! Stopping...", existing, topic);
            return;
        }
        if (runFlag) {
            log.info("already listening to topic: {}!", topic);
            return;
        }
        runFlag = true;
        consumerProperties.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, ("consumer-kafka-trilobita-" + UUID.randomUUID()));
        consumerThread = new Thread(() -> {
            try (Consumer<String, Mail> consumer = new KafkaConsumer<>(consumerProperties)) {
                consumer.subscribe(Collections.singletonList(topic));
                while (runFlag) {
                    ConsumerRecords<String, Mail> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, Mail> consumerRecord : records) {
//                        UUID key = consumerRecord.key();
                        Mail value = consumerRecord.value();
                        int partition = consumerRecord.partition();
                        long offset = consumerRecord.offset();
                        log.info("Consumer Record: Topic: {}, key: {}, value: {}, partition: {}, offset: {}",
                                topic, consumerRecord.key(), value, partition, offset
                        );
                        messageHandler.handleMessage(UUID.fromString(consumerRecord.key()), value, partition, offset);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        consumerThread.start();
    }

    /**
     * <p>Stop the consumer thread.
     * </p>
     *
     * @author Guo Ziniu: ziniu@catroll.io
     * @see MessageConsumer#stop(boolean)
     */
    public void stop() throws InterruptedException {
        stop(false);
    }

    /**
     * <p>Stop the consumer thread.
     * </p>
     *
     * @param block If set to true, it will wait for the current task to finish.
     * @author Guo Ziniu: ziniu@catroll.io
     */
    public void stop(boolean block) throws InterruptedException {
        runFlag = false;
        if (block) {
            consumerThread.join();
        }
        consumerThread = null;
    }

    /**
     * <p>Change the topic and restart consumer.
     * </p>
     *
     * @param topic The topic to be changed to.
     * @author Guo Ziniu: ziniu@catroll.io
     */
    public void setTopic(String topic) throws InterruptedException, ExecutionException {
        this.topic = topic;
        stop(true);
        start();
    }
}
