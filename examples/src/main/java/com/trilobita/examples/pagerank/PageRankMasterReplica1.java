package com.trilobita.examples.pagerank;

import com.trilobita.engine.server.masterserver.partition.Partitioner;
import com.trilobita.engine.server.masterserver.partition.strategy.PartitionStrategy;
import com.trilobita.engine.server.masterserver.partition.strategy.PartitionStrategyFactory;
import com.trilobita.engine.server.util.functionable.Functionable;
import com.trilobita.engine.server.util.functionable.instance.combiner.SumCombiner;
import com.trilobita.examples.pagerank.vertex.PageRankValue;
import com.trilobita.runtime.environment.TrilobitaEnvironment;

import java.util.concurrent.ExecutionException;

public class PageRankMasterReplica1 {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        TrilobitaEnvironment<Double> trilobitaEnvironment = new TrilobitaEnvironment<>();
        trilobitaEnvironment.initConfig();
        trilobitaEnvironment.loadGraph(PageRankMasterRunner.createVerticesFromJson());
        PartitionStrategyFactory partitionStrategyFactory = new PartitionStrategyFactory();
        PartitionStrategy partitionStrategy = partitionStrategyFactory.getPartitionStrategy("hashPartitionStrategy",(int) trilobitaEnvironment.getConfiguration().get("numOfWorker"),trilobitaEnvironment.getGraph().getSize());
        trilobitaEnvironment.setPartitioner(new Partitioner<>(partitionStrategy));
        Functionable.FunctionableRepresenter[] funcs = {
                new Functionable.FunctionableRepresenter(SumCombiner.class.getName(), null, new PageRankValue(0.0), new PageRankValue(0.0)),
        };
//        trilobitaEnvironment.createMasterServer(1, 10, false,funcs);
        trilobitaEnvironment.createMasterServer(0, 10, false);
        trilobitaEnvironment.startMasterServer();
    }
}
