package pagerank;

import com.trilobita.engine.server.masterserver.partitioner.HashPartitionStrategy;
import com.trilobita.engine.server.masterserver.partitioner.Partioner;
import com.trilobita.runtime.launcher.TrilobitaEnvironment;
import pagerank.vertex.PageRankValue;

import java.util.concurrent.ExecutionException;

public class PageRankWorker2 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        TrilobitaEnvironment<PageRankValue> trilobitaEnvironment = new TrilobitaEnvironment<>();
        trilobitaEnvironment.initConfig();
        trilobitaEnvironment.createWorkerServer(2);
        trilobitaEnvironment.startWorkerServer();
    }
}
