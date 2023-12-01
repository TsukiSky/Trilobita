package com.trilobita.examples.shortestpath;

import com.trilobita.runtime.environment.TrilobitaEnvironment;
import com.trilobita.examples.shortestpath.vertex.ShortestPathValue;

import java.util.concurrent.ExecutionException;

public class ShortestPathWorker2 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        TrilobitaEnvironment<Double> trilobitaEnvironment = new TrilobitaEnvironment<>();
        trilobitaEnvironment.initConfig();
        trilobitaEnvironment.createWorkerServer(2);
        trilobitaEnvironment.startWorkerServer();
    }
}
