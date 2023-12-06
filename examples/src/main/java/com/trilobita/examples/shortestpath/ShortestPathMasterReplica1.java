package com.trilobita.examples.shortestpath;

import com.trilobita.core.graph.Graph;
import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.engine.server.masterserver.partition.strategy.PartitionStrategy;
import com.trilobita.engine.server.masterserver.partition.strategy.PartitionStrategyFactory;
import com.trilobita.engine.server.masterserver.partition.Partitioner;
import com.trilobita.runtime.environment.TrilobitaEnvironment;
import com.trilobita.examples.shortestpath.vertex.ShortestPathValue;
import com.trilobita.examples.shortestpath.vertex.ShortestPathVertex;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class ShortestPathMasterReplica1 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        TrilobitaEnvironment<Double> trilobitaEnvironment = new TrilobitaEnvironment<>();
        trilobitaEnvironment.initConfig();
        trilobitaEnvironment.loadGraph(ShortestPathMasterRunner.createVerticesFromFile());
        PartitionStrategyFactory partitionStrategyFactory = new PartitionStrategyFactory();
        PartitionStrategy partitionStrategy = partitionStrategyFactory.getPartitionStrategy("hashPartitionStrategy",(int) trilobitaEnvironment.getConfiguration().get("numOfWorker"),trilobitaEnvironment.getGraph().getSize());
        trilobitaEnvironment.setPartitioner(new Partitioner<>(partitionStrategy));
        trilobitaEnvironment.createMasterServer(1, 10, false);
        trilobitaEnvironment.startMasterServer();
    }
}
