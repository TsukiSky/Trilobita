package com.trilobita.engine.server.masterserver;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trilobita.commons.*;
import com.trilobita.core.graph.Graph;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.core.messaging.MessageConsumer;
import com.trilobita.core.messaging.MessageProducer;
import com.trilobita.engine.server.AbstractServer;
import com.trilobita.engine.server.masterserver.partitioner.AbstractPartitioner;
import com.trilobita.engine.server.masterserver.partitioner.HashPartitioner;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Master Server is the master of a server cluster, coordinate the start and the end of a Superstep
 */
@Slf4j
public class MasterServer<T> extends AbstractServer<T> {
    Graph graph;
    AbstractPartitioner<T> graphPartitioner;
    Integer nRunningWorkers;
    Integer nPauseWorkers;
    Integer nDownWorkers;
    AtomicInteger finishedWorkers;
    volatile Boolean handlingFault;
    List<Integer> workerIds;
    MessageConsumer completeSignalListener;
    ConcurrentHashMap<Integer, Computable<T>> curVertexValue;
    ConcurrentHashMap<Integer, Computable<T>> newVertexValue;
    ConcurrentHashMap<Integer, Boolean> workerStatus;
    ScheduledExecutorService executorService;


    private static MasterServer<?> instance;

    private MasterServer(int serverId) throws ExecutionException, InterruptedException {
        super(serverId);
        finishedWorkers = new AtomicInteger(0);
        executorService = Executors.newScheduledThreadPool(1);
        handlingFault = false;
        curVertexValue = new ConcurrentHashMap<>();
        newVertexValue = new ConcurrentHashMap<>();
        completeSignalListener = new MessageConsumer("finish", serverId, new MessageConsumer.MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail value, int partition, long offset) throws JsonProcessingException, InterruptedException {
                if (handlingFault){
                    return;
                }
                int val = finishedWorkers.addAndGet(1);
                ConcurrentHashMap<Integer, Computable<T>> tempValue = (ConcurrentHashMap<Integer, Computable<T>>) value.getMessage().getContent();
                updateVertexValue(tempValue);
                log.info("finished number of workers: "+val);
                if (val == nDownWorkers){
                    // start next superstep
                    finishedWorkers.set(0);
                    curVertexValue = newVertexValue;
                    Thread.sleep(300);
                    log.info(String.valueOf(curVertexValue));
                    MessageProducer.produce(null, new Mail(-1, null, Mail.MailType.NORMAL), "start");
                }
            }
        });
        completeSignalListener.start();
        checkHeartBeat();
    }

    public void updateVertexValue(ConcurrentHashMap<Integer, Computable<T>> tempValue){
        Set<Map.Entry<Integer, Computable<T>>> set = tempValue.entrySet();
        for (Map.Entry<Integer, Computable<T>> entry: set){
            System.out.println(entry.getKey() + " " + entry.getValue());
            newVertexValue.put(entry.getKey(), entry.getValue());
        }
    }

    public void checkHeartBeat(){
        executorService.schedule(new Runnable() {
            @Override
            public void run() {
                //  get the id of the dead server
                Integer serverId = checkStatus();
                if (serverId == -1){
                    return;
                }
                log.info(String.valueOf(serverId));
            }
        }, 2, TimeUnit.SECONDS);
    }

    public Integer checkStatus(){
        Set<Map.Entry<Integer, Boolean>> set = workerStatus.entrySet();
        for (Map.Entry<Integer, Boolean> entry: set){
            if (!entry.getValue()){
                return entry.getKey();
            }
        }
        return -1;
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

    public void partitionGraph(Graph graph, Integer nWorkers) {
        nDownWorkers = nWorkers;
        List<VertexGroup<T>> vertexGroupArrayList;
        AbstractPartitioner<T> partitioner = new HashPartitioner(nWorkers);
        vertexGroupArrayList = partitioner.Partition(graph, nWorkers);
//        create a hashmap that store (key: vertexId, value: serverId)
        Map<Integer, Integer> vertexToServer = new HashMap<>();
        for (int i=1;i<vertexGroupArrayList.size();i++){
            for (Vertex v: vertexGroupArrayList.get(i-1).getVertices()){
                vertexToServer.put(v.getId(), i);
            }
        }
        for (int i=1;i<=vertexGroupArrayList.size();i++){
            Map<String, Object> objectMap = new HashMap<>();
            objectMap.put("VERTEX-TO-SERVER", vertexToServer);
            objectMap.put("PARTITION", vertexGroupArrayList.get(i-1));
            Message message = new Message(objectMap, Message.MessageType.NULL);
            Mail mail = new Mail(-1, message, Mail.MailType.GRAPH_PARTITION);
            MessageProducer.produce(null, mail, i+"partition");
        }
    }
}
