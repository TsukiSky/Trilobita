package com.trilobita.examples.shortestpath;

import com.trilobita.examples.shortestpath.vertex.ShortestPathValue;
import com.trilobita.runtime.environment.TrilobitaEnvironment;

import java.util.concurrent.ExecutionException;

public class ShortestPathWorker1 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        TrilobitaEnvironment<Double> trilobitaEnvironment = new TrilobitaEnvironment<>();
        trilobitaEnvironment.initConfig();
        trilobitaEnvironment.createWorkerServer(1);
        trilobitaEnvironment.startWorkerServer();
    }
}
