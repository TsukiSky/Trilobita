package com.trilobita.examples.shortestpath;

import com.trilobita.engine.server.masterserver.partition.Partitioner;
import com.trilobita.engine.server.masterserver.partition.strategy.PartitionStrategy;
import com.trilobita.engine.server.masterserver.partition.strategy.PartitionStrategyFactory;
import com.trilobita.engine.server.util.functionable.Functionable;
import com.trilobita.engine.server.util.functionable.instance.aggregator.DifferenceAggregator;
import com.trilobita.engine.server.util.functionable.instance.combiner.MinCombiner;
import com.trilobita.examples.shortestpath.vertex.ShortestPathValue;
import com.trilobita.runtime.environment.TrilobitaEnvironment;

import java.util.concurrent.ExecutionException;

public class ShortestPathMasterReplica1 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        TrilobitaEnvironment<Double> trilobitaEnvironment = new TrilobitaEnvironment<>();
        trilobitaEnvironment.initConfig();
        trilobitaEnvironment.loadGraph(ShortestPathMasterRunner.createVertices());
        PartitionStrategyFactory partitionStrategyFactory = new PartitionStrategyFactory();
        PartitionStrategy partitionStrategy = partitionStrategyFactory.getPartitionStrategy("hashPartitionStrategy",(int) trilobitaEnvironment.getConfiguration().get("numOfWorker"),trilobitaEnvironment.getGraph().getSize());
        trilobitaEnvironment.setPartitioner(new Partitioner<>(partitionStrategy));
        Functionable.FunctionableRepresenter[] funcs = {
                new Functionable.FunctionableRepresenter(MinCombiner.class.getName(), null, new ShortestPathValue(Double.POSITIVE_INFINITY),new ShortestPathValue(Double.POSITIVE_INFINITY)),
                new Functionable.FunctionableRepresenter(DifferenceAggregator.class.getName(), "DIFF_AGG", new ShortestPathValue(Double.POSITIVE_INFINITY),new ShortestPathValue(Double.POSITIVE_INFINITY))
        };
//        trilobitaEnvironment.createMasterServer(1, 10, false, funcs);
        trilobitaEnvironment.createMasterServer(1, 10, false);
        trilobitaEnvironment.startMasterServer();
    }
}
