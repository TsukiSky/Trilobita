package com.trilobita.core.messaging;

import com.trilobita.core.common.util.Util;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

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
    public Set<String> getTopic() throws ExecutionException, InterruptedException {
        ListTopicsResult listTopics = adminClient.listTopics();
        return listTopics.names().get();
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
        Set<String> existing = getTopic();
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
}
