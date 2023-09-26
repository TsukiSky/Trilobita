package com.trilobita.core.messaging;

import com.trilobita.commons.Mail;
import com.trilobita.commons.MailType;
import com.trilobita.commons.Message;
import com.trilobita.core.common.util.Util;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;

import javax.naming.Context;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Handler;

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
        void handleMessage(UUID key, Mail value, int partition, long offset);
    }

    public MessageConsumer(String topic, MessageHandler messageHandler) {
        consumerProperties.putAll(messageAdmin.props);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-trilobita");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
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
        Set<String> existing = messageAdmin.getTopic();
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
            try (Consumer<UUID, Mail> consumer = new KafkaConsumer<>(consumerProperties)) {
                consumer.subscribe(Collections.singletonList(topic));
                while (runFlag) {
                    ConsumerRecords<UUID, Mail> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<UUID, Mail> consumerRecord : records) {
                        UUID key = consumerRecord.key();
                        Mail value = consumerRecord.value();
                        int partition = consumerRecord.partition();
                        long offset = consumerRecord.offset();
                        log.info("Consumer Record: Topic: {}, key: {}, value: {}, partition: {}, offset: {}",
                                topic, key, value, partition, offset
                        );
                        messageHandler.handleMessage(key, value, partition, offset);
                    }
                }
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