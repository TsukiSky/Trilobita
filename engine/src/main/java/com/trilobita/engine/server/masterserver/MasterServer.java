package com.trilobita.engine.server.masterserver;

import com.trilobita.commons.*;
import com.trilobita.core.graph.Graph;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.core.messaging.MessageConsumer;
import com.trilobita.core.messaging.MessageProducer;
import com.trilobita.engine.server.AbstractServer;
import com.trilobita.engine.server.functionable.FunctionalMailsHandler;
import com.trilobita.engine.server.heartbeat.HeartbeatChecker;
import com.trilobita.engine.server.heartbeat.HeartbeatSender;
import com.trilobita.engine.server.masterserver.partitioner.Partioner;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Master Server is the master of a server cluster, coordinate the start and the end of a Superstep
 */
@Slf4j
public class MasterServer<T> extends AbstractServer<T> {
    Graph<T> graph;                                     // the graph to be computed
    Partioner<T> graphPartitioner;            // the partitioner of the graph
    int nWorker;                                        // the number of workers
    AtomicInteger nFinishedWorker;                      // the number of workers that have finished the superstep
    MessageConsumer completeSignalConsumer;             // the consumer that consume the finish signal from workers
    ConcurrentHashMap<Integer, Boolean> workerStatus;   // the status of the workers
    HeartbeatChecker workerHeartbeatChecker;
    HeartbeatChecker masterHeartbeatChecker;
    HeartbeatSender heartbeatSender;
    ValueSnapshot<T> valueSnapshot;
    List<Integer> WorkingWorkerIdList;                  // the alive working server's id
    FunctionalMailsHandler functionalMailsHandler;

    public MasterServer(Partioner<T> graphPartitioner, int nWorker,int id, FunctionalMailsHandler functionalMailsHandler) {
        super(0, graphPartitioner.getPartitionStrategy());   // the standard server id of master is 0
        this.nWorker = nWorker;
        this.graphPartitioner = graphPartitioner;
        this.nFinishedWorker = new AtomicInteger(0);
        this.valueSnapshot = new ValueSnapshot<>();
        this.heartbeatSender = new HeartbeatSender(getServerId(), false);
        this.workerHeartbeatChecker = new HeartbeatChecker(new ArrayList<>(), true, new HeartbeatChecker.FaultHandler() {
            @Override
            public void handleFault() {
                // call the repartition function of the worker
            }
        });

        this.masterHeartbeatChecker = new HeartbeatChecker(new ArrayList<>(), false, new HeartbeatChecker.FaultHandler() {
            @Override
            public void handleFault() {
                // if all id with greater id has died, become the master
            }
        });
        this.completeSignalConsumer = new MessageConsumer(Mail.MailType.FINISH_SIGNAL.ordinal(), super.getServerId(), new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail value, int partition, long offset) throws InterruptedException {
                if (workerHeartbeatChecker.getIsProcessing()){
                    return;
                }
                int val = nFinishedWorker.addAndGet(1);
                log.info("number of finished workers: " + val);
                HashMap<Integer, Computable<T>> vertexValue = (HashMap<Integer, Computable<T>>) value.getMessage().getContent();
                log.info(vertexValue.toString());
                valueSnapshot.record(vertexValue);
                if (val == nWorker) {
                    // start the next superstep
                    valueSnapshot.finishSuperstep(graph);
                    // todo: update the new graph to other replicas
                    Thread.sleep(300);
                    startNewSuperstep();
                }
            }
        });
        this.functionalMailsHandler = functionalMailsHandler;
    }

    @Override
    public void start() throws ExecutionException, InterruptedException {
        this.completeSignalConsumer.start();
        this.workerHeartbeatChecker.start();
        this.partitionGraph();
        this.heartbeatSender.start();

    }

    @Override
    public void pause() {
    }

    @Override
    public void shutdown() {
    }

    public void finish(){

    }

    /**
     * start a new round of superstep
     */
    public void startNewSuperstep() {
        this.superstep += 1;
        this.nFinishedWorker.set(0);
        MessageProducer.produceStartSignal();
        // TODO: send functionalMails to vertices in workers
    }

    /**
     * partition the graph and send the partitioned graph to the workers
     */
    public void partitionGraph() {
        if (this.graph == null) {
            throw new Error("graph is not set!");
        }
        List<VertexGroup<T>> vertexGroupArrayList;
        vertexGroupArrayList = this.graphPartitioner.partition(graph, nWorker);

        for (int i = 1; i <= vertexGroupArrayList.size(); i++) {
            Map<String, Object> objectMap = new HashMap<>();
            objectMap.put("PARTITION", vertexGroupArrayList.get(i - 1));
            objectMap.put("PARTITIONSTRATEGY",this.graphPartitioner.getPartitionStrategy());
            Message message = new Message(objectMap);
            Mail mail = new Mail(-1, message, Mail.MailType.PARTITION);
            MessageProducer.createAndProduce(null, mail, "SERVER_" + i + "_PARTITION");
        }
    }

    /**
     * load the graph to the master server
     * @param graph the graph to be loaded
     */
    public void setGraph(Graph<T> graph) {
        this.graph = graph;
        valueSnapshot.setGraph(graph);
    }

    public void repartition (){
        List<VertexGroup<T>> vertexGroupArrayList;
        vertexGroupArrayList = this.graphPartitioner.partition(graph, WorkingWorkerIdList.size());

        for (int i = 1; i <= vertexGroupArrayList.size(); i++) {
            Map<String, Object> objectMap = new HashMap<>();
            objectMap.put("PARTITION", vertexGroupArrayList.get(i - 1));
            objectMap.put("PARTITIONSTRATEGY",this.graphPartitioner.getPartitionStrategy());
            Message message = new Message(objectMap);
            Mail mail = new Mail(-1, message, Mail.MailType.PARTITION);
            MessageProducer.createAndProduce(null, mail, "SERVER_" + WorkingWorkerIdList.get(i) + "_PARTITION");
        }
    }
}
