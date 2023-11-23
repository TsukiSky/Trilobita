package shortestpath;

import com.trilobita.runtime.environment.TrilobitaEnvironment;
import shortestpath.vertex.ShortestPathValue;

import java.util.concurrent.ExecutionException;

public class ShortestPathWorker1 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        TrilobitaEnvironment<ShortestPathValue> trilobitaEnvironment = new TrilobitaEnvironment<>();
        trilobitaEnvironment.initConfig();
        trilobitaEnvironment.createWorkerServer(1);
        trilobitaEnvironment.startWorkerServer();
    }
}
