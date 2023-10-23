package com.trilobita.engine.server.masterserver;

import com.trilobita.commons.Mail;
import com.trilobita.commons.MailType;
import com.trilobita.commons.Message;
import com.trilobita.commons.MessageType;
import com.trilobita.core.graph.Graph;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.core.messaging.MessageAdmin;
import com.trilobita.core.messaging.MessageProducer;
import com.trilobita.engine.server.AbstractServer;
import com.trilobita.engine.server.masterserver.partitioner.AbstractPartitioner;
import com.trilobita.engine.server.masterserver.partitioner.Partitioner;
import com.trilobita.engine.server.workerserver.WorkerServer;
import java.util.ArrayList;
import java.util.List;

/**
 * Master Server is the master of a server cluster, coordinate the start and the end of a Superstep
 */
public class MasterServer extends AbstractServer {
    Graph graph;
    AbstractPartitioner graphPartitioner;
    WorkerServer[] workerCluster;
    Integer nRunningWorkers;
    Integer nPauseWorkers;
    Integer nDownWorkers;
    List<Integer> workerIds;

    private static MasterServer instance;

    private MasterServer(int serverId) {
        super(serverId);
    }

    public static synchronized MasterServer getInstance() {
        if (instance == null) {
            instance = new MasterServer(0);
            instance.initialize();
        }
        return instance;
    }

    @Override
    public void initialize() {}

    @Override
    public void start() {}

    @Override
    public void pause() {}

    @Override
    public void shutdown() {}

    public void onCompleteSignal(Integer workerId) {}

    public void sendStartSignal() {}

    public void partitionGraph(Graph graph, Integer nWorkers) {
        ArrayList<VertexGroup> vertexGroupArrayList;
        Partitioner partitioner = new Partitioner();
        vertexGroupArrayList = partitioner.Partition(graph, nWorkers);
//        todo: Send partitions to workers
        for (int i=0;i<vertexGroupArrayList.size();i++){
            Message message = new Message(vertexGroupArrayList.get(i),MessageType.NULL);
            Mail mail = new Mail(-1, message, MailType.GRAPH_PARTITION);
            MessageProducer.produce(null, mail, i+"partition");
        }
    }
}
