package pagerank;

import com.trilobita.core.graph.Graph;
import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.engine.server.masterserver.partition.Partitioner;
import com.trilobita.engine.server.masterserver.partition.strategy.PartitionStrategy;
import com.trilobita.engine.server.masterserver.partition.strategy.PartitionStrategyFactory;
import com.trilobita.engine.server.util.functionable.examples.ExampleFunctionable;
import com.trilobita.engine.server.util.functionable.examples.aggregators.MinValueAggregator;
import com.trilobita.engine.server.util.functionable.examples.combiners.MaxCombiner;
import com.trilobita.runtime.environment.TrilobitaEnvironment;
import pagerank.vertex.PageRankValue;
import pagerank.vertex.PageRankVertex;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class PageRankMasterRunner {
    public static Graph<Double> createVertices(){
        List<Vertex<Double>> vertices = new ArrayList<>();
        PageRankVertex vertex0 = new PageRankVertex(0);
        vertex0.setStatus(Vertex.VertexStatus.ACTIVE);
        vertices.add(vertex0);

        PageRankVertex vertex1 = new PageRankVertex(1);
        vertex1.setStatus(Vertex.VertexStatus.ACTIVE);
        vertices.add(vertex1);

        PageRankVertex vertex2 = new PageRankVertex(2);
        vertex2.setStatus(Vertex.VertexStatus.ACTIVE);
        vertices.add(vertex2);

        PageRankVertex vertex3 = new PageRankVertex(3);
        vertex3.setStatus(Vertex.VertexStatus.ACTIVE);
        vertices.add(vertex3);

        PageRankVertex vertex4 = new PageRankVertex(4);
        vertex4.setStatus(Vertex.VertexStatus.ACTIVE);
        vertices.add(vertex4);

        PageRankVertex vertex5 = new PageRankVertex(5);
        vertex5.setStatus(Vertex.VertexStatus.ACTIVE);
        vertices.add(vertex5);

        PageRankVertex vertex6 = new PageRankVertex(6);
        vertex6.setStatus(Vertex.VertexStatus.ACTIVE);
        vertices.add(vertex6);

        PageRankVertex vertex7 = new PageRankVertex(7);
        vertex7.setStatus(Vertex.VertexStatus.ACTIVE);
        vertices.add(vertex7);

        PageRankVertex vertex8 = new PageRankVertex(8);
        vertex8.setStatus(Vertex.VertexStatus.ACTIVE);
        vertices.add(vertex8);

        PageRankVertex vertex9 = new PageRankVertex(9);
        vertex9.setStatus(Vertex.VertexStatus.ACTIVE);
        vertices.add(vertex9);

        vertex1.addEdge(vertex2);
        vertex2.addEdge(vertex4);
        vertex4.addEdge(vertex3);
        vertex3.addEdge(vertex4);
        vertex1.addEdge(vertex3);
        vertex7.addEdge(vertex3);
        vertex6.addEdge(vertex3);
        vertex9.addEdge(vertex3);
        vertex5.addEdge(vertex3);
        vertex7.addEdge(vertex8);
        vertex8.addEdge(vertex7);
        vertex0.addEdge(vertex8);
        vertex0.addEdge(vertex5);
        vertex0.addEdge(vertex9);
        vertex9.addEdge(vertex6);

        return new Graph<>(vertices);
    }
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        TrilobitaEnvironment<Double> trilobitaEnvironment = new TrilobitaEnvironment<>();
        trilobitaEnvironment.initConfig();
        trilobitaEnvironment.loadGraph(PageRankMasterRunner.createVertices());
        PartitionStrategyFactory partitionStrategyFactory = new PartitionStrategyFactory();
        PartitionStrategy partitionStrategy = partitionStrategyFactory.getPartitionStrategy("hashPartitionStrategy",(int) trilobitaEnvironment.getConfiguration().get("numOfWorker"),trilobitaEnvironment.getGraph().getSize());
        trilobitaEnvironment.setPartitioner(new Partitioner<>(partitionStrategy));
        ExampleFunctionable[] funcs = {
            new ExampleFunctionable(MinValueAggregator.class.getName(), "MIN_VAL_AGG", new PageRankValue(0.0)),
                    new ExampleFunctionable(MaxCombiner.class.getName(), null, new PageRankValue(Double.NEGATIVE_INFINITY))
        };
        trilobitaEnvironment.createMasterServer(0, 10,true,funcs);
        trilobitaEnvironment.startMasterServer();
    }
}
