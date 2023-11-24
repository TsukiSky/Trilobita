package com.trilobita.core.messaging;

import com.trilobita.core.common.Util;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Slf4j
public class MessageAdmin {
    private static MessageAdmin instance = null;
    final Properties props = Util.loadConfig("core/src/main/resources/kafka.properties");
    private final AdminClient adminClient;

    private MessageAdmin() {
        this.adminClient = AdminClient.create(props);
    }

    public static synchronized MessageAdmin getInstance() {
        if (instance == null)
            instance = new MessageAdmin();
        return instance;
    }

    /**
     * <p>Return all the existing topics.
     * </p>
     *
     * @return All the existing topics.
     * @author Guo Ziniu: ziniu@catroll.io
     */
    public Set<String> getTopics() throws ExecutionException, InterruptedException {
        ListTopicsResult listTopics = adminClient.listTopics();
        return listTopics.names().get();
    }

    /**
     * <p>Return all the consumer groups that subscribe to given topic.
     * </p>
     *
     * @param topic the topic which you want to get consumer groups for
     * @return All the consumer groups that subscribe to given topic.
     * @author Guo Ziniu: ziniu@catroll.io
     */
    public Set<String> getConsumerGroupsForTopic(String topic) throws ExecutionException, InterruptedException {
        Set<String> consumerGroups = adminClient.listConsumerGroups().all().get().stream()
                .map(ConsumerGroupListing::groupId)
                .collect(Collectors.toSet());
        Set<String> ret = new HashSet<>();
        for (String group : consumerGroups) {
            DescribeConsumerGroupsResult describeResult = adminClient.describeConsumerGroups(Collections.singletonList(group));
            KafkaFuture<Map<String, ConsumerGroupDescription>> future = describeResult.all();
            Map<String, ConsumerGroupDescription> res = future.get();
            for (Map.Entry<String, ConsumerGroupDescription> entry : res.entrySet()) {
                ConsumerGroupDescription description = entry.getValue();
                for (MemberDescription memberDescription : description.members()) {
                    if (memberDescription.assignment().topicPartitions().stream()
                            .anyMatch(tp -> tp.topic().equals(topic))) {

                        log.info("Group " + group + " is subscribed to topic " + topic);
                        ret.add(group);
                    }
                }
            }
        }
        return ret;
    }

    /**
     * <p>Return all the consumer groups and their consumers that subscribe to given topic.
     * </p>
     *
     * @param topic the topic which you want to get consumers for
     * @return All the consumer groups that subscribe to given topic, and their consumers.
     * @author Guo Ziniu: ziniu@catroll.io
     */
    public Map<String, Set<String>> getConsumersForTopic(String topic) throws ExecutionException, InterruptedException {
        Map<String, Set<String>> groupToConsumersMap = new HashMap<>();
        Set<String> consumerGroups = adminClient.listConsumerGroups().all().get().stream()
                .map(ConsumerGroupListing::groupId)
                .collect(Collectors.toSet());
        for (String group : consumerGroups) {
            DescribeConsumerGroupsResult describeResult = adminClient.describeConsumerGroups(Collections.singletonList(group));
            KafkaFuture<Map<String, ConsumerGroupDescription>> future = describeResult.all();
            Map<String, ConsumerGroupDescription> res = future.get();
            for (Map.Entry<String, ConsumerGroupDescription> entry : res.entrySet()) {
                ConsumerGroupDescription description = entry.getValue();
                Set<String> consumersInGroup = new HashSet<>();
                for (MemberDescription memberDescription : description.members()) {
                    if (memberDescription.assignment().topicPartitions().stream()
                            .anyMatch(tp -> tp.topic().equals(topic))) {
                        log.debug("Consumer " + memberDescription.consumerId() + " in group " + group + " is subscribed to topic " + topic);
                        consumersInGroup.add(memberDescription.consumerId());
                    }
                }
                if (!consumersInGroup.isEmpty()) {
                    groupToConsumersMap.put(group, consumersInGroup);
                }
            }
        }
        return groupToConsumersMap;
    }

    /**
     * <p>Create a topic if it does not exist.
     * </p>
     *
     * @param topic       the topic that you want to create
     * @param partitions  partitions of the topic
     * @param replication replication of the topic
     * @author Guo Ziniu: ziniu@catroll.io
     */
    public void createIfNotExist(String topic, int partitions, short replication) throws ExecutionException, InterruptedException {
        Set<String> existing = getTopics();
        if (existing.contains(topic)) {
            return;
        }
        List<NewTopic> topicList = new ArrayList<>();
        NewTopic newTopic = new NewTopic(topic, partitions, replication);
        topicList.add(newTopic);
        CreateTopicsResult result = adminClient.createTopics(topicList);
        KafkaFuture<Void> future = result.values().get(topic);
        future.get();
    }

    /**
     * <p>Create a topic if it does not exist.
     * </p>
     *
     * @author Guo Ziniu: ziniu@catroll.io
     * @see MessageAdmin#createIfNotExist(String, int, short)
     */
    public void createIfNotExist(String topic) throws ExecutionException, InterruptedException {
        createIfNotExist(topic, 1, (short) 3);
    }

    public void deleteIfExist(String topic) throws ExecutionException, InterruptedException {
        Set<String> existing = getTopics();
        if (!existing.contains(topic)) {
            return;
        }
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Collections.singleton(topic));
        KafkaFuture<Void> future = deleteTopicsResult.all();
        future.get();
    }

    /**
     * Clear a topic by deleting and recreating it.
     *
     * @param topic the topic to be cleared
     */
    public void purgeTopic(String topic) throws ExecutionException, InterruptedException {
        deleteIfExist(topic);
        createIfNotExist(topic);
    }

    /**
     * Clear all the topics
     */
    public void purgeAllTopics() throws ExecutionException, InterruptedException {
        Set<String> topics = getTopics();
        for (String topic : topics) {
            purgeTopic(topic);
        }
    }

    /**
     * Clear all the topics by deleting them
     */
    public void deleteAllTopics() throws ExecutionException, InterruptedException {
        Set<String> topics = getTopics();
        for (String topic : topics) {
            deleteIfExist(topic);
        }
    }

    public boolean existTopic(String topic) {
        try {
            return getTopics().contains(topic);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
            return false;
        }
    }
}
