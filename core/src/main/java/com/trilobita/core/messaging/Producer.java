package com.trilobita.core.messaging;

import com.trilobita.core.common.util.Util;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class Producer {

    private static AdminClient admin = null;
    private static Properties props = Util.loadConfig("core/src/main/resources/kafka.properties");


    private static AdminClient getAdmin() {
        if (admin == null) {
            admin = AdminClient.create(props);
        }
        return admin;
    }

    public static void getTopic(String topic) {
        try (AdminClient admin = getAdmin()) {
            ListTopicsResult listTopics = admin.listTopics();
            Set<String> names = listTopics.names().get();
            System.out.println("existing topics: " + names.toString());

//            int partitions = 1;
//            short replicationFactor = 1;
//            NewTopic newTopic = new NewTopic(topic, partitions, replicationFactor);
//
//            CreateTopicsResult result = admin.createTopics(
//                    Collections.singleton(newTopic)
//            );
//
//            KafkaFuture<Void> future = result.values().get(topic);
//            future.get();
        } catch (ExecutionException | InterruptedException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void produce() throws IOException {
        Object key = "key";
        Object value = new Object();
        final String topic = "topic_0";
        try (final org.apache.kafka.clients.producer.Producer<Object, Object> producer = new KafkaProducer<>(props)) {
            producer.send(
                    new ProducerRecord<>(topic, key, value),
                    (event, ex) -> {
                        if (ex != null)
                            ex.printStackTrace();
                        else
                            System.out.printf("Produced event to topic %s: key = %-10s value = %s%n", topic, key, value);
                    });
        }
    }
}