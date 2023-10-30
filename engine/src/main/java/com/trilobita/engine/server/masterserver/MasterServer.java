package com.trilobita.engine.server.masterserver;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trilobita.commons.Mail;
import com.trilobita.commons.Message;
import com.trilobita.core.graph.Graph;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.core.messaging.MessageConsumer;
import com.trilobita.core.messaging.MessageProducer;
import com.trilobita.engine.server.AbstractServer;
import com.trilobita.engine.server.masterserver.partitioner.AbstractPartitioner;
import com.trilobita.engine.server.masterserver.partitioner.HashPartitioner;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Master Server is the master of a server cluster, coordinate the start and the end of a Superstep
 */
@Slf4j
public class MasterServer<T> extends AbstractServer<T> {
    Graph<T> graph;
    AbstractPartitioner graphPartitioner;
    Integer nRunningWorkers;
    Integer nPauseWorkers;
    Integer nDownWorkers;
    AtomicInteger finishedWorkers;
    List<Integer> workerIds;
    MessageConsumer completeSignalListener;

    private static MasterServer<?> instance;

    private MasterServer(int serverId) throws ExecutionException, InterruptedException {
        super(serverId);
        finishedWorkers = new AtomicInteger(0);
        completeSignalListener = new MessageConsumer("finish", serverId, new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail value, int partition, long offset) throws JsonProcessingException, InterruptedException {
                int val = finishedWorkers.addAndGet(1);
                log.info("finished number of workers: "+val);
                if (val == nDownWorkers){
                    // start next superstep
                    finishedWorkers.set(0);
                    Thread.sleep(300);
                    MessageProducer.produce(null, new Mail(-1, null, Mail.MailType.NORMAL), "start");
                }
            }
        });
        completeSignalListener.start();
    }

    public static synchronized MasterServer<?> getInstance() throws ExecutionException, InterruptedException {
        if (instance == null) {
            instance = new MasterServer<>(0);
        }
        return instance;
    }
    @Override
    public void start() {}

    @Override
    public void pause() {}

    @Override
    public void shutdown() {}

    public void onCompleteSignal(Integer workerId) {}

    public void sendStartSignal() {}

    public void partitionGraph(Graph<T> graph, Integer nWorkers) {
        nDownWorkers = nWorkers;
        List<VertexGroup<T>> vertexGroupArrayList;
        AbstractPartitioner<T> partitioner = new HashPartitioner(nWorkers);
        vertexGroupArrayList = partitioner.Partition(graph, nWorkers);
//        todo: Send partitions to workers
        for (int i=1;i<=vertexGroupArrayList.size();i++){
            System.out.println(i);
            Message message = new Message(vertexGroupArrayList.get(i-1), Message.MessageType.NULL);
            Mail mail = new Mail(-1, message, Mail.MailType.GRAPH_PARTITION);
            MessageProducer.produce(null, mail, i+"partition");
        }
    }
}
