package com.trilobita.runtime.launcher;

import com.trilobita.core.graph.Graph;
import com.trilobita.engine.server.functionable.Functionable;
import com.trilobita.engine.server.functionable.FunctionalMailsHandler;
import com.trilobita.engine.server.masterserver.MasterServer;
import com.trilobita.engine.server.masterserver.partitioner.Partioner;
import com.trilobita.engine.server.masterserver.partitioner.PartitionStrategy;
import com.trilobita.engine.server.workerserver.WorkerServer;
import com.trilobita.runtime.configuration.Configuration;
import com.trilobita.runtime.configuration.JCommandHandler;
import com.trilobita.runtime.launcher.inputparser.Parse;
import lombok.Getter;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * The running environment for Trilobita Job
 */
public class TrilobitaEnvironment<T> {
    public static TrilobitaEnvironment<?> trilobitaEnvironment;
    private Parse inputParser;
    @Getter
    private final Configuration configuration = new Configuration();
    private final JCommandHandler jCommandHandler = new JCommandHandler();  // Command-line handler for Trilobita
    private Graph<T> graph;
    public PartitionStrategy partitionStrategy;
    public Partioner<T> partitioner;
    public MasterServer<T> masterServer;
    public WorkerServer<?> workerServer;

    public TrilobitaEnvironment() {
    }

    /**
     * Initialize the configuration of Trilobita
     */
    public void initConfig() {
        jCommandHandler.initConfig(configuration);
    }

    public void loadGraph(Graph<T> graph) {
        this.graph = graph;
    }

    public Graph<T> getGraph(){
        return this.graph;
    }

    public void setPartitioner(Partioner<T> partitioner) {
        this.partitioner = partitioner;
        this.partitionStrategy = partitioner.getPartitionStrategy();
    }

    public void setInputParser(Parse inputParser) {
        this.inputParser = inputParser;
    }

    public void createMasterServer(FunctionalMailsHandler functionalMailsHandler) {
        this.masterServer = new MasterServer<>(this.partitioner, (int) this.configuration.get("numOfWorker"), 0,functionalMailsHandler);
        this.masterServer.setGraph(this.graph);
    }

    public void createWorkerServer(int workerId, List<Functionable> functionables) throws ExecutionException, InterruptedException {
        this.workerServer = new WorkerServer<>(workerId, (int) this.configuration.get("parallelism"), this.partitionStrategy, functionables);
    }

    public void startMasterServer() throws ExecutionException, InterruptedException {
        this.masterServer.start();
    }

    public void startWorkerServer() throws InterruptedException, ExecutionException {
        this.workerServer.start();
    }
}
