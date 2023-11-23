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

        ShortestPathVertex vertex3 = new ShortestPathVertex(3);
        vertex3.setStatus(Vertex.VertexStatus.INACTIVE);
        vertices.add(vertex3);

        ShortestPathVertex vertex4 = new ShortestPathVertex(4);
        vertex4.setStatus(Vertex.VertexStatus.INACTIVE);
        vertices.add(vertex4);

        ShortestPathVertex vertex5 = new ShortestPathVertex(5);
        vertex5.setStatus(Vertex.VertexStatus.INACTIVE);
        vertices.add(vertex5);

        ShortestPathVertex vertex6 = new ShortestPathVertex(6);
        vertex6.setStatus(Vertex.VertexStatus.INACTIVE);
        vertices.add(vertex6);

        ShortestPathVertex vertex7 = new ShortestPathVertex(7);
        vertex7.setStatus(Vertex.VertexStatus.INACTIVE);
        vertices.add(vertex7);


        vertex0.addEdge(vertex1,new ShortestPathValue((double) 3));
        vertex0.addEdge(vertex2,new ShortestPathValue((double) 10));
        vertex1.addEdge(vertex2,new ShortestPathValue((double) 6));
        vertex2.addEdge(vertex1,new ShortestPathValue((double) 1));
        vertex2.addEdge(vertex3,new ShortestPathValue((double) 5));
        vertex1.addEdge(vertex3,new ShortestPathValue((double) 3));
        vertex3.addEdge(vertex4,new ShortestPathValue((double) 2));
        vertex3.addEdge(vertex5,new ShortestPathValue((double) 4));
        vertex4.addEdge(vertex6,new ShortestPathValue((double) 10));
        vertex5.addEdge(vertex6,new ShortestPathValue((double) 8));
        vertex0.addEdge(vertex7,new ShortestPathValue((double) 7));
        vertex7.addEdge(vertex5,new ShortestPathValue((double) 2));


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
