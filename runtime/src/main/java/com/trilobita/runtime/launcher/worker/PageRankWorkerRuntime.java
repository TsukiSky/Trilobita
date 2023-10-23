package com.trilobita.runtime.launcher.worker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.engine.server.workerserver.WorkerServer;

import java.util.concurrent.ExecutionException;

public class PageRankWorkerRuntime {

    public static void main(String[] args) throws ExecutionException, InterruptedException, JsonProcessingException {
//        for (int i=0;i<4;i++){
//            WorkerServer workerServer = new WorkerServer(i, 2);
//            workerServer.initialize();
//            workerServer.start();
//        }

        WorkerServer workerServer = new WorkerServer(0, 2);

    }
}
