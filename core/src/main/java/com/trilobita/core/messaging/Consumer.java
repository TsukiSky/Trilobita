package com.trilobita.core.messaging;

import com.trilobita.core.common.Util;
import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    private static volatile boolean runFlag;

    public static void start() throws Exception {
        Consumer.runFlag = true;
        new Thread(
                () -> {
                    try {
                        consume();
                    } catch (Exception e) {
                    }
                }
        ).start();

    }

    public static void stop() {
        Consumer.runFlag = false;
    }

    public static void consume() throws Exception {

        final String topic = "topic_0";

        final Properties props = Util.loadConfig("core/src/main/resources/kafka.properties");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-java-getting-started");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (final org.apache.kafka.clients.consumer.Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Arrays.asList(topic));
            while (runFlag) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String key = record.key();
                    String value = record.value();
                    System.out.println(String.format("Consumed event from topic %s: key = %-10s value = %s", topic, key, value));
                }
            }
        }
    }

}
