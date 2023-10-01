package com.trilobita.server;

import java.util.concurrent.BlockingQueue;

import com.trilobita.commons.Mail;
import com.trilobita.core.graph.VertexGroup;

import lombok.Data;

/*
 * Stores metadata of a server, including vertexGroup, in/outMailQueue.
 */
@Data
public class Context {
    // public Integer serverId;
    private VertexGroup vertexGroup;
    private BlockingQueue<Mail> outMailQueue;
    private BlockingQueue<Mail> inMailQueue;

}
