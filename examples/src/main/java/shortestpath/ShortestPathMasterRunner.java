package shortestpath;

import com.trilobita.core.graph.Graph;
import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.engine.server.masterserver.partitioner.Partitioner;
import com.trilobita.engine.server.masterserver.partitioner.PartitionStrategy;
import com.trilobita.engine.server.masterserver.partitioner.PartitionStrategyFactory;
import com.trilobita.runtime.environment.TrilobitaEnvironment;
import shortestpath.vertex.ShortestPathValue;
import shortestpath.vertex.ShortestPathVertex;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class ShortestPathMasterRunner {
    public static Graph createVertices(){
        List<ShortestPathVertex> vertices = new ArrayList<>();
        ShortestPathVertex vertex0 = new ShortestPathVertex(0,0.0,true);
        vertex0.setStatus(Vertex.VertexStatus.ACTIVE);
        vertices.add(vertex0);

        ShortestPathVertex vertex1 = new ShortestPathVertex(1);
        vertex1.setStatus(Vertex.VertexStatus.INACTIVE);
        vertices.add(vertex1);

        ShortestPathVertex vertex2 = new ShortestPathVertex(2);
        vertex2.setStatus(Vertex.VertexStatus.INACTIVE);
        vertices.add(vertex2);

        vertex0.addEdge(vertex1,new ShortestPathValue((double) 3));
        vertex0.addEdge(vertex2,new ShortestPathValue((double) 10));
        vertex1.addEdge(vertex2,new ShortestPathValue((double) 6));
        vertex2.addEdge(vertex1,new ShortestPathValue((double) 1));

        Graph<ShortestPathValue> graph = new Graph(vertices);
        return graph;
    }
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        TrilobitaEnvironment<ShortestPathValue> trilobitaEnvironment = new TrilobitaEnvironment<>();
        trilobitaEnvironment.initConfig();
        Graph g = ShortestPathMasterRunner.createVertices();
        trilobitaEnvironment.loadGraph(g);
        PartitionStrategyFactory partitionStrategyFactory = new PartitionStrategyFactory();
        PartitionStrategy partitionStrategy = partitionStrategyFactory.getPartitionStrategy("hashPartitionStrategy",(int) trilobitaEnvironment.getConfiguration().get("numOfWorker"),trilobitaEnvironment.getGraph().getSize());
        trilobitaEnvironment.setPartitioner(new Partitioner<>(partitionStrategy));
        trilobitaEnvironment.createMasterServer(2,10);
        trilobitaEnvironment.startMasterServer();
    }
}
