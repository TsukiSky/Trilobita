package com.trilobita.core.messaging;

import com.trilobita.core.common.util.Util;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;

import java.util.*;
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

    private static void getTopic(String topic) {
        try (AdminClient adminClient = getAdmin()) {
            ListTopicsResult listTopics = adminClient.listTopics();
            Set<String> names = listTopics.names().get();
            System.out.println("existing topics: " + names.toString());

            boolean contains = names.contains(topic);
            if (!contains) {
                System.out.println("CREATING!!!!");
                List<NewTopic> topicList = new ArrayList<>();
                int partitions = 1;
                short replication = 3;
                NewTopic newTopic = new NewTopic(topic, partitions, replication);
                topicList.add(newTopic);
                CreateTopicsResult result = adminClient.createTopics(topicList);
                KafkaFuture<Void> future = result.values().get(topic);
                future.get();
                System.out.println("CREATING????");
            }
        } catch (ExecutionException | InterruptedException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void produce(Object key, Object value, String topic) {
        getTopic(topic);
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