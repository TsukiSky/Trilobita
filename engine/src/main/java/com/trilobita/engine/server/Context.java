package com.trilobita.engine.server;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

import com.trilobita.commons.Mail;
import com.trilobita.core.graph.VertexGroup;

import lombok.Data;

/*
 * Stores metadata of a server, including vertexGroup, in/outMailQueue.
 */
@Data
public class Context {
    private Integer serverId;
    private AbstractServer.ServerStatus serverStatus;
    private Map<Integer, Integer> vertexToServer;
    private VertexGroup vertexGroup;
    private ConcurrentHashMap<Integer, CopyOnWriteArrayList<Mail>> outMailTable;
    private final LinkedBlockingQueue<Mail> outMailQueue;
    private final LinkedBlockingQueue<Mail> inMailQueue;
    protected Integer superstep;
}
