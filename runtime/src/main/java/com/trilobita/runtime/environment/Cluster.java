package com.trilobita.runtime.environment;

import com.trilobita.engine.server.masterserver.MasterServer;
import com.trilobita.engine.server.workerserver.WorkerServer;

import java.util.List;

public class Cluster<T> {
    private MasterServer<T> masterServer;
    private List<WorkerServer<T>> workerServers;
}
