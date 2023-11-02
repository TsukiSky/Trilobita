package com.trilobita.examples.worker;

import com.trilobita.engine.server.masterserver.partitioner.AbstractPartitioner;
import com.trilobita.engine.server.masterserver.partitioner.HashPartitioner;
import com.trilobita.engine.server.workerserver.WorkerServer;
import com.trilobita.examples.impl.PageRankValue;

import java.util.concurrent.ExecutionException;

public class PageRankWorker2 {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        AbstractPartitioner.PartitionStrategy partitionStrategy = new HashPartitioner<>(2).getPartitionStrategy();
        WorkerServer<PageRankValue> workerServer = new WorkerServer<>(2, 2, partitionStrategy);
        workerServer.start();
    }
}
