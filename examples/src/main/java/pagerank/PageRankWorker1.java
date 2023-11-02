package pagerank;

import com.trilobita.engine.server.masterserver.partitioner.HashPartitioner;
import com.trilobita.runtime.launcher.TrilobitaEnvironment;
import pagerank.vertex.PageRankValue;

import java.util.concurrent.ExecutionException;

public class PageRankWorker1 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        TrilobitaEnvironment<PageRankValue> trilobitaEnvironment = new TrilobitaEnvironment<>();
        trilobitaEnvironment.initConfig();
        trilobitaEnvironment.setPartitioner(new HashPartitioner<>((int) trilobitaEnvironment.getConfiguration().get("numOfWorker")));
        trilobitaEnvironment.createWorkerServer(1);
        trilobitaEnvironment.startWorkerServer();
    }
}
