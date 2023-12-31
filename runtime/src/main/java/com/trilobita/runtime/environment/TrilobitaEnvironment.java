package com.trilobita.runtime.environment;

import com.trilobita.core.graph.Graph;
import com.trilobita.engine.server.masterserver.MasterServer;
import com.trilobita.engine.server.masterserver.partition.Partitioner;
import com.trilobita.engine.server.masterserver.partition.strategy.PartitionStrategy;
import com.trilobita.engine.server.util.functionable.Functionable;
import com.trilobita.engine.server.workerserver.WorkerServer;
import com.trilobita.runtime.configuration.Configuration;
import com.trilobita.runtime.configuration.JCommandHandler;
//import com.trilobita.runtime.parser.inputparser.InputParse;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutionException;

/**
 * The running environment for Trilobita Job
 */
@Slf4j
public class TrilobitaEnvironment<T> {
    public static TrilobitaEnvironment<?> trilobitaEnvironment;
//    private InputParse inputParser;
    @Getter
    private final Configuration configuration = new Configuration();
    private final JCommandHandler jCommandHandler = new JCommandHandler(); // Command-line handler for Trilobita
    @Getter
    private Graph<T> graph;
    public PartitionStrategy partitionStrategy;
    public Partitioner<T> partitioner;
    public MasterServer<T> masterServer;
    public WorkerServer<T> workerServer;

    public TrilobitaEnvironment() {}

    /**
     * Initialize the configuration of Trilobita
     */
    public void initConfig() {
        log.info("[TE] TrilobitaEnvironment initialized");
        jCommandHandler.initConfig(configuration);
    }

    public void loadGraph(Graph<T> graph) {
        this.graph = graph;
    }

    public void setPartitioner(Partitioner<T> partitioner) {
        this.partitioner = partitioner;
        this.partitionStrategy = partitioner.getPartitionStrategy();
    }
    
    public void createMasterServer(int id, int snapshotFrequency, boolean isPrimary, Functionable.FunctionableRepresenter[] functionables)
            throws ExecutionException, InterruptedException {
        this.masterServer = new MasterServer<>(this.partitioner, (int) this.configuration.get("numOfWorker"), id, (int) this.configuration.get("numOfReplica"), snapshotFrequency, isPrimary);
        this.masterServer.setGraph(this.graph);
        this.masterServer.setFunctionables(functionables);
    }

    public void createMasterServer(int id, int snapshotFrequency, boolean isPrimary) throws ExecutionException, InterruptedException {
        this.createMasterServer(id, snapshotFrequency, isPrimary, null);
    }

    public void createMasterServer(int id, int snapshotFrequency) throws ExecutionException, InterruptedException {
        this.createMasterServer(id, snapshotFrequency, false, null);
    }


    public void join(String clusterName) {

    }


    public void createWorkerServer(int workerId) throws ExecutionException, InterruptedException {
        this.workerServer = new WorkerServer<>(workerId, (int) this.configuration.get("parallelism"), this.partitionStrategy);
    }

    public void startMasterServer() {
        this.masterServer.start();
    }

    public void startWorkerServer() throws InterruptedException, ExecutionException {
        this.workerServer.start();
    }
}
