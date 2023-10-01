package com.trilobita.engine.server;

import com.trilobita.commons.Address;
import com.trilobita.commons.Mail;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.engine.server.common.ServerStatus;
import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * The abstract parent class for all server instances
 */
@Getter
public abstract class AbstractServer {
    private final Integer serverId;   // unique id of a server
    private final Address address;
    @Setter
    private ServerStatus serverStatus;
    @Setter
    private VertexGroup vertexGroup;
    private final BlockingQueue<Mail> outMailQueue;
    private final BlockingQueue<Mail> inMailQueue;

    public AbstractServer(int serverId, Address address) {
        this.serverId = serverId;
        this.address = address;
        this.outMailQueue = new LinkedBlockingQueue<>();
        this.inMailQueue = new LinkedBlockingQueue<>();
    }

    public abstract void start();
    public abstract void pause();
    public abstract void shutdown();

    // server initialize
    public abstract void initialize();

    public void post() {
        // post all mails to its destination
        // TODO: implement post method for server
    }

    public void postMail() {
        // post one mail to its destination
        // TODO: implement single mail post method for server
    }
}
