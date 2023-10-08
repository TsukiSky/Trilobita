package com.trilobita.engine.server;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

import com.trilobita.commons.Mail;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.engine.server.common.ServerStatus;

import lombok.Data;

/*
 * Stores metadata of a server, including vertexGroup, in/outMailQueue.
 */
@Data
public class Context {
    public Integer serverId;
    public ServerStatus serverStatus;
    private VertexGroup vertexGroup;
    private ConcurrentHashMap<Integer, CopyOnWriteArrayList<Mail>> outMailTable;
    private final LinkedBlockingQueue<Mail> outMailQueue;
    private final LinkedBlockingQueue<Mail> inMailQueue;

}
