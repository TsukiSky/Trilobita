package com.trilobita.engine.server;

import com.trilobita.commons.Mail;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.core.messaging.MessageConsumer;
import com.trilobita.core.messaging.MessageProducer;
import com.trilobita.engine.server.common.ServerStatus;
import com.trilobita.exception.TrilobitaException;
import lombok.Getter;
import lombok.Setter;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * The abstract parent class for all server instances
 */
@Getter
public abstract class AbstractServer<T> {
    private final Integer serverId;   // unique id of a server
    @Setter
    private ServerStatus serverStatus;
    @Setter
    protected volatile VertexGroup vertexGroup;
    private final LinkedBlockingQueue<Mail> outMailQueue;
    private final LinkedBlockingQueue<Mail> inMailQueue;
    private final MessageConsumer messageConsumer;
    public Context context;

    protected AbstractServer(int serverId) {
        this.serverId = serverId;
        this.outMailQueue = new LinkedBlockingQueue<>();
        this.inMailQueue = new LinkedBlockingQueue<>();
        this.messageConsumer = new MessageConsumer(serverId+"", new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail value, int partition, long offset) {
                AbstractServer.this.inMailQueue.add(value);
            }
        });
    }

    public abstract void start() throws TrilobitaException, InterruptedException, ExecutionException;
    public abstract void pause();
    public abstract void shutdown();

    public void post() {
        // post all mails to its destination
        // TODO: implement post method for server
    }

    public int findServerByVertexId(int vertexId) {
        // TODO: do findServerByVertexId
        return 0;
    }

    public void postMail(Mail mail) {
        // post one mail to its destination
        MessageProducer.produce(null, mail, "//TODOTOPIC");
    }
}
