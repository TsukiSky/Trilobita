package com.trilobita.engine.server;

import com.trilobita.core.common.Mail;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.core.messaging.MessageConsumer;
import com.trilobita.core.messaging.MessageProducer;
import com.trilobita.engine.server.masterserver.partition.strategy.PartitionStrategy;
import com.trilobita.core.exception.TrilobitaException;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * The abstract parent class for all server instances
 */
@Getter
@Slf4j
public abstract class AbstractServer<T> {
    public final Integer serverId;   // unique id of a server
    @Setter
    public ServerStatus serverStatus;
    @Setter
    public VertexGroup<T> vertexGroup;
    public final LinkedBlockingQueue<Mail> outMailQueue;
    public final LinkedBlockingQueue<Mail> inMailQueue;
    public final MessageConsumer messageConsumer;
    @Setter
    public PartitionStrategy partitionStrategy;

    public AbstractServer(int serverId, PartitionStrategy partitionStrategy) {
        this.serverId = serverId;
        this.partitionStrategy = partitionStrategy;
        this.outMailQueue = new LinkedBlockingQueue<>();
        this.inMailQueue = new LinkedBlockingQueue<>();
        this.messageConsumer = new MessageConsumer("SERVER_" + serverId + "_MESSAGE", serverId, new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail value, int partition, long offset) {
                AbstractServer.this.inMailQueue.add(value);
                MessageProducer.produce(null, new Mail(), "VALUE_RECEIVE_CONFIRM");
            }
        });
    }

    public abstract void start() throws TrilobitaException, InterruptedException, ExecutionException;
    public abstract void pause() throws InterruptedException;
    public abstract void shutdown() throws InterruptedException;

    /**
     * find the server id of a vertex
     * @param vertexId the id of the vertex
     * @return the server id of the vertex
     */
    public int findServerByVertexId(int vertexId) {
        return partitionStrategy.getServerIdByVertexId(vertexId);
    }

    /**
     * STATUS of Server
     */
    public enum ServerStatus {
        RUNNING, PAUSE, SHUTDOWN
    }
}
