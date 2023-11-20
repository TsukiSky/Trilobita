package com.trilobita.runtime.environment;

import com.trilobita.core.graph.Graph;
import com.trilobita.engine.server.masterserver.MasterServer;
import com.trilobita.engine.server.masterserver.partitioner.Partioner;
import com.trilobita.engine.server.masterserver.partitioner.PartitionStrategy;
import com.trilobita.engine.server.workerserver.WorkerServer;
import com.trilobita.runtime.configuration.Configuration;
import com.trilobita.runtime.configuration.JCommandHandler;
import com.trilobita.runtime.parser.inputparser.InputParse;
import lombok.Getter;

import java.util.concurrent.ExecutionException;

/**
 * The running environment for Trilobita Job
 */
public class TrilobitaEnvironment<T> {
    public static TrilobitaEnvironment<?> trilobitaEnvironment;
    private InputParse inputParser;
    @Getter
    private final Configuration configuration = new Configuration();
    private final JCommandHandler jCommandHandler = new JCommandHandler();  // Command-line handler for Trilobita
    @Getter
    private Graph<T> graph;
    public PartitionStrategy partitionStrategy;
    public Partioner<T> partitioner;
    public MasterServer<T> masterServer;
    public WorkerServer<T> workerServer;
    public Cluster<T> cluster;

    public TrilobitaEnvironment() {}

    /**
     * Initialize the configuration of Trilobita
     */
    public void initConfig() {
        jCommandHandler.initConfig(configuration);
    }

    public void loadGraph(Graph<T> graph) {
        this.graph = graph;
    }

    public void setPartitioner(Partioner<T> partitioner) {
        this.partitioner = partitioner;
        this.partitionStrategy = partitioner.getPartitionStrategy();
    }

    public void setInputParser(InputParse inputParser) {
        this.inputParser = inputParser;
    }

    public void createMasterServer(int id) throws ExecutionException, InterruptedException {
        this.masterServer = new MasterServer<>(this.partitioner, (int) this.configuration.get("numOfWorker"), id, (int) this.configuration.get("numOfReplica"));
        this.masterServer.setGraph(this.graph);
    }

    public void join(String clusterName) {

    }


    public void createWorkerServer(int workerId) throws ExecutionException, InterruptedException {
        this.workerServer = new WorkerServer<>(workerId, (int) this.configuration.get("parallelism"), this.partitionStrategy);
    }

    public void startMasterServer() throws ExecutionException, InterruptedException {
        this.masterServer.start();
    }

    public void startWorkerServer() throws InterruptedException, ExecutionException {
        this.workerServer.start();
    }
}
