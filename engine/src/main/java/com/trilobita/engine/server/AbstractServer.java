package com.trilobita.engine.server;

import com.trilobita.commons.Address;
import com.trilobita.commons.Mail;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.engine.server.common.ServerStatus;
import com.trilobita.engine.server.scheduler.AbstractScheduler;

import java.util.concurrent.BlockingQueue;

/**
 * The abstract parent class for all server instances
 */
public abstract class AbstractServer {
    private Integer serverId;   // unique id of a server
    private Address address;
    private ServerStatus serverStatus;
    private AbstractScheduler scheduler;
    private VertexGroup vertexGroup;
    private BlockingQueue<Mail> outMailQueue;
    private BlockingQueue<Mail> inMailQueue;

    public abstract void start();
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
