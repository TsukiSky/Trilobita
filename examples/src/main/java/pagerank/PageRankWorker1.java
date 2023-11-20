package pagerank;

import com.trilobita.runtime.environment.TrilobitaEnvironment;
import pagerank.vertex.PageRankValue;

import java.util.concurrent.ExecutionException;

public class PageRankWorker1 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        TrilobitaEnvironment<PageRankValue> trilobitaEnvironment = new TrilobitaEnvironment<>();
        trilobitaEnvironment.initConfig();
        trilobitaEnvironment.createWorkerServer(1);
        trilobitaEnvironment.startWorkerServer();
    }
}
