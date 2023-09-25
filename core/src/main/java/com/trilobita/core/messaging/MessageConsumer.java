package com.trilobita.core.messaging;

import com.trilobita.core.common.util.Util;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public interface MessageConsumer {

    void start();

    void stop();

    Properties props = Util.loadConfig("core/src/main/resources/kafka.properties");

    @Slf4j
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
                props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,("consumer-kafka-trilobita-"+ UUID.randomUUID()));
                try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
                    consumer.subscribe(Collections.singletonList(topic));
                    while (runFlag) {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                        for (ConsumerRecord<String, String> consumerRecord : records) {
                            log.info("Consumer Record: Topic: {}, key: {}, value: {}, partition: {}, offset: {}",
                                    topic, consumerRecord.key(), consumerRecord.value(),
                                    consumerRecord.partition(), consumerRecord.offset());
                        }
                    }
                }
            }).start();
        }

        @Override
        public void stop() {
            runFlag = false;
        }

    }

}