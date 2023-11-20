package com.trilobita.engine.server.masterserver;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trilobita.commons.*;
import com.trilobita.core.graph.Graph;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.core.messaging.MessageConsumer;
import com.trilobita.core.messaging.MessageProducer;
import com.trilobita.engine.server.AbstractServer;
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
    MessageConsumer heatBeatConsumer;
    ConcurrentHashMap<Integer, Boolean> workerStatus;   // the status of the workers
    HeartbeatChecker workerHeartbeatChecker;
    HeartbeatChecker masterHeartbeatChecker;
    HeartbeatSender heartbeatSender;
    ValueSnapshot<T> valueSnapshot;
    List<Integer> workingWorkerIdList;                  // the alive working server's id

    public MasterServer(Partioner<T> graphPartitioner, int nWorker, int id) {
        super(id, graphPartitioner.getPartitionStrategy());   // the standard server id of master is 0
        this.nWorker = nWorker;
        this.graphPartitioner = graphPartitioner;
        this.nFinishedWorker = new AtomicInteger(0);
        this.valueSnapshot = new ValueSnapshot<>();
        this.heartbeatSender = new HeartbeatSender(getServerId(), false);
        this.workingWorkerIdList = new ArrayList<>();
        for (int i=0;i<nWorker;i++){
            this.workingWorkerIdList.add(i+1);
        }
        this.workerHeartbeatChecker = new HeartbeatChecker(this.workingWorkerIdList, true, new HeartbeatChecker.FaultHandler() {
            @Override
            public void handleFault(int id) {
                workingWorkerIdList.remove((Integer) id);
                nFinishedWorker.set(0);
                MasterServer.this.partitionGraph();
                log.info("finished repartitioning...");
            }
        });

        this.masterHeartbeatChecker = new HeartbeatChecker(new ArrayList<>(), false, new HeartbeatChecker.FaultHandler() {
            @Override
            public void handleFault(int id) {
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
                valueSnapshot.record(vertexValue);
                if (val == workingWorkerIdList.size()) {
                    // start the next superstep
                    valueSnapshot.finishSuperstep(graph);
                    log.info("graph is : {}", graph.getVertices());
                    // todo: update the new graph to other replicas
                    Thread.sleep(300);
                    startNewSuperstep();
                }
            }
        });
        heatBeatConsumer = new MessageConsumer("HEARTBEAT_WORKER", getServerId(), new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail value, int partition, long offset) {
                int id = (int) value.getMessage().getContent();
                workerHeartbeatChecker.setHeatBeat(id);
            }
        });
    }

    @Override
    public void start() throws ExecutionException, InterruptedException {
        this.completeSignalConsumer.start();
        this.workerHeartbeatChecker.start();
        this.partitionGraph();
        this.heartbeatSender.start();
        this.heatBeatConsumer.start();

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
    }

    /**
     * partition the graph and send the partitioned graph to the workers
     */
    public void partitionGraph() {
        if (this.graph == null) {
            throw new Error("graph is not set!");
        }
        Map<Integer, VertexGroup<T>> map;
        map = this.graphPartitioner.partition(graph, workingWorkerIdList);

        Set<Map.Entry<Integer, VertexGroup<T>>> set = map.entrySet();
        for (Map.Entry<Integer, VertexGroup<T>> entry: set) {
            Map<String, Object> objectMap = new HashMap<>();
            objectMap.put("PARTITION", entry.getValue());
            objectMap.put("PARTITIONSTRATEGY",this.graphPartitioner.getPartitionStrategy());
            Message message = new Message(objectMap);
            Mail mail = new Mail(-1, message, Mail.MailType.PARTITION);
            MessageProducer.createAndProduce(null, mail, "SERVER_" + entry.getKey() + "_PARTITION");
        }
    }

    /**
     * load the graph to the master server
     * @param graph the graph to be loaded
     */
    public void setGraph(Graph<T> graph) {
        System.out.println(graph.getVertices());
        this.graph = graph;
    }
//
//    public void repartition (){
//        Map<Integer, VertexGroup<T>> map;
//        map = this.graphPartitioner.partition(graph, workingWorkerIdList);
//
//        Set<Map.Entry<Integer, VertexGroup<T>>> set = map.entrySet();
//        for (Map.Entry<Integer, VertexGroup<T>> entry: set) {
//            Map<String, Object> objectMap = new HashMap<>();
//            objectMap.put("PARTITION", entry.getValue());
//            objectMap.put("PARTITIONSTRATEGY",this.graphPartitioner.getPartitionStrategy());
//            Message message = new Message(objectMap);
//            Mail mail = new Mail(-1, message, Mail.MailType.PARTITION);
//            MessageProducer.createAndProduce(null, mail, "SERVER_" + entry.getKey() + "_PARTITION");
//        }
//    }
}
