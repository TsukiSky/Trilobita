package com.trilobita.core.messaging;

import com.trilobita.core.common.util.Util;
import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public interface MessageConsumer {

    void start();

    void stop();

    void consume();
    Properties props = Util.loadConfig("core/src/main/resources/kafka.properties");

    class DefaultMessageConsumer implements MessageConsumer {
        private boolean runFlag = false;
        String topic;

        public DefaultMessageConsumer(String selfTopic) {
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-trilobita");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            this.topic = selfTopic;
        }

        @Override
        public void start() {
            runFlag = true;
            new Thread(() -> {
                consume();
            }).start();
        }

        @Override
        public void stop() {
            runFlag = false;
        }

        @Override
        public void consume() {
            try (final org.apache.kafka.clients.consumer.Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(Arrays.asList(topic));
                while (runFlag) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
                                record.key(), record.value(),
                                record.partition(), record.offset());
                    }
                }
            }
        }
    }

}