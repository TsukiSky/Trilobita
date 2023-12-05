package com.trilobita.examples.pagerank;

import com.trilobita.runtime.environment.TrilobitaEnvironment;

import java.util.concurrent.ExecutionException;

public class PageRankWorker2 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        TrilobitaEnvironment<Double> trilobitaEnvironment = new TrilobitaEnvironment<>();
        trilobitaEnvironment.initConfig();
        trilobitaEnvironment.createWorkerServer(2);
        trilobitaEnvironment.startWorkerServer();
    }
}
