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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

/**
 * <h1>MessageConsumer</h1>
 * Consume messages from a certain Kafka topic.
 *
 * @author Guo Ziniu: ziniu@catroll.io
 */
@Slf4j
public class MessageConsumer {
    private static final boolean DEBUG_LOG = false;
    private volatile boolean runFlag = false;
    private String topic;
    private final MessageAdmin messageAdmin = MessageAdmin.getInstance();
    private final Properties consumerProperties = new Properties();
    private Thread consumerThread;
    private final MessageHandler messageHandler;

    public interface MessageHandler {
        /**
         * Handle message received from a topic.
         * Note: This method should not handle complicated tasks as it will block the consumer(listener) thread!
         * It is recommended to add messages to a {@link BlockingQueue}.
         *
         * @param key       Message {@link UUID}
         * @param value     {@link Mail}, which is the message payload
         * @param partition message partition
         * @param offset    message offset
         * @author Guo Ziniu: ziniu@catroll.io
         */
        void handleMessage(UUID key, Mail value, int partition, long offset) throws JsonProcessingException, InterruptedException, ExecutionException;
    }

    public MessageConsumer(int topic, Integer serverId, MessageHandler messageHandler) {
        this(String.valueOf(topic), serverId, messageHandler);
    }

    public MessageConsumer(String topic, Integer serverId, MessageHandler messageHandler) {
        consumerProperties.putAll(messageAdmin.props);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-kafka-trilobita-"+ serverId + "-" + UUID.randomUUID()); // Master topic probably is subscribed by multiple workers.
        consumerProperties.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, ("consumer-kafka-trilobita-" + topic)); // one worker has multiple consumer (group instance) differentiated by topic.
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        this.messageHandler = messageHandler;
        this.topic = topic;
    }

    public MessageConsumer(String topic, Integer serverId, String offsetPolicy, MessageHandler messageHandler) {
        consumerProperties.putAll(messageAdmin.props);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-kafka-trilobita-"+ serverId+"-"+UUID.randomUUID()); // Master topic probably is subscribed by multiple workers.
        consumerProperties.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, ("consumer-kafka-trilobita-" + topic)); // one worker has multiple consumer (group instance) differentiated by topic.
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetPolicy);
        this.messageHandler = messageHandler;
        this.topic = topic;
    }


    /**
     * Start listening to the topic in a new thread
     * @author Guo Ziniu: ziniu@catroll.io
     */
    public void start() throws ExecutionException, InterruptedException {
        Set<String> existing = messageAdmin.getTopics();
        if (!existing.contains(topic)) {
            messageAdmin.createIfNotExist(topic);
            log.info("existing topic: {} do not contain {}! Creating, and then subscribe...", existing, topic);
        }
        if (runFlag) {
            log.info("already listening to topic: {}!", topic);
            return;
        }
        runFlag = true;
        CountDownLatch latch = new CountDownLatch(1);
        consumerThread = new Thread(() -> {
            try (Consumer<String, Mail> consumer = new KafkaConsumer<>(consumerProperties)) {
                consumer.subscribe(Collections.singletonList(topic));
                latch.countDown();
                while (runFlag) {
                    ConsumerRecords<String, Mail> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, Mail> consumerRecord : records) {
//                        UUID key = consumerRecord.key();
                        Mail value = consumerRecord.value();
                        int partition = consumerRecord.partition();
                        long offset = consumerRecord.offset();
                        if (DEBUG_LOG){
                            log.info("Consumer Record: Topic: {}, key: {}, value: {}, partition: {}, offset: {}",
                                    topic, consumerRecord.key(), value, partition, offset
                            );
                        }
//                        if (value != null){
                        messageHandler.handleMessage(UUID.fromString(consumerRecord.key()), value, partition, offset);
//                        }
                    }
                }
            } catch (ExecutionException | JsonProcessingException | InterruptedException e) {
                log.error("[MessageConsumer]", e);
            }
        });
        consumerThread.start();
        latch.await();
    }

    /**
     * Stop the consumer thread.
     * @author Guo Ziniu: ziniu@catroll.io
     * @see MessageConsumer#stop(boolean)
     */
    public void stop() throws InterruptedException {
        stop(false);
    }

    /**
     * Stop the consumer thread.
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
     * Change the topic and restart consumer.
     * @param topic The topic to be changed to.
     * @author Guo Ziniu: ziniu@catroll.io
     */
    public void setTopic(String topic) throws InterruptedException, ExecutionException {
        this.topic = topic;
        stop(true);
        start();
    }
}
