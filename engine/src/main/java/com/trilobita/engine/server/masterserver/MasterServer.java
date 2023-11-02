package com.trilobita.engine.server.masterserver;

import com.trilobita.commons.*;
import com.trilobita.core.graph.Graph;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.core.messaging.MessageConsumer;
import com.trilobita.core.messaging.MessageProducer;
import com.trilobita.engine.server.AbstractServer;
import com.trilobita.engine.server.masterserver.partitioner.AbstractPartitioner;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Master Server is the master of a server cluster, coordinate the start and the end of a Superstep
 */
@Slf4j
public class MasterServer<T> extends AbstractServer<T> {
    Graph<T> graph;
    AbstractPartitioner<T> graphPartitioner;
    int nWorker;
    AtomicInteger nFinishedWorker;
    MessageConsumer completeSignalConsumer;
    ConcurrentHashMap<Integer, Boolean> workerStatus;

    public MasterServer(AbstractPartitioner<T> graphPartitioner, int nWorker) throws ExecutionException, InterruptedException {
        super(0);   // the standard server id of master is 0
        this.nWorker = nWorker;
        this.graphPartitioner = graphPartitioner;
        this.nFinishedWorker = new AtomicInteger(0);
        this.completeSignalConsumer = new MessageConsumer(Mail.MailType.FINISH_SIGNAL.ordinal(), super.getServerId(), new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail value, int partition, long offset) throws InterruptedException {
                int val = nFinishedWorker.addAndGet(1);
                log.info("number of finished workers: " + val);
                if (val == nWorker) {
                    // start the next superstep
                    Thread.sleep(300);
                    startNewSuperstep();
                }
            }
        });
    }

    @Override
    public void start() throws ExecutionException, InterruptedException {
        this.completeSignalConsumer.start();
    }

    @Override
    public void pause() {
    }

    @Override
    public void shutdown() {
    }

    public void startNewSuperstep() {
        this.superstep += 1;
        this.nFinishedWorker.set(0);
        MessageProducer.produceStartSignal();
    }

    /**
     * partition the graph and send the partitioned graph to the workers
     * @param graph the graph to be partitioned
     * @param nWorker the number of workers
     */
    public void partitionGraph(Graph<T> graph, Integer nWorker) {
        this.nWorker = nWorker;
        this.graph = graph;
        List<VertexGroup<T>> vertexGroupArrayList;
        vertexGroupArrayList = this.graphPartitioner.Partition(graph, nWorker);

        // create a hashmap that store (key: vertexId, value: serverId)
        Map<Integer, Integer> vertexToServer = new HashMap<>();
        for (int i = 1; i <= vertexGroupArrayList.size(); i++) {
            for (Vertex<T> v :vertexGroupArrayList.get(i - 1).getVertices()) {
                vertexToServer.put(v.getId(), i);
            }
        }
        for (int i = 1; i <= vertexGroupArrayList.size(); i++) {
            Map<String, Object> objectMap = new HashMap<>();
            objectMap.put("VERTEX-TO-SERVER", vertexToServer);
            objectMap.put("PARTITION", vertexGroupArrayList.get(i - 1));
            Message message = new Message(objectMap);
            Mail mail = new Mail(-1, message, Mail.MailType.PARTITION);
            MessageProducer.createAndProduce(null, mail, "SERVER_" + i + "_PARTITION");
        }
    }
}
