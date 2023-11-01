package com.trilobita.examples.worker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trilobita.engine.server.workerserver.WorkerServer;

import java.util.concurrent.ExecutionException;

public class PageRankWorkerRunner2 {

    public static void main(String[] args) throws ExecutionException, InterruptedException, JsonProcessingException {
//        for (int i=0;i<4;i++){
//            WorkerServer workerServer = new WorkerServer(i, 2);
//            workerServer.initialize();
//            workerServer.start();
//        }

        WorkerServer workerServer = new WorkerServer(2, 2);

    }
}
