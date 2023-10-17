package com.trilobita.runtime.launcher.Worker;

import com.trilobita.engine.server.workerserver.WorkerServer;

public class PageRankWorkerRuntime {
    public static void main(String[] args) {
        for (int i=0;i<4;i++){
            WorkerServer workerServer = new WorkerServer(i, 2);
            workerServer.initialize();
            workerServer.start();
        }
    }
}
