package com.trilobita.engine.server.functionable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.trilobita.commons.Computable;
import com.trilobita.commons.Mail;
import com.trilobita.commons.Message;
import com.trilobita.commons.Mail.MailType;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.engine.server.Context;

/*
 * Monitor and communicate vertices metadata on a workerserver.
 * Each vertex can provide a value to an aggregator in superstep S, 
 * the system combines those values using a reduction operator, and 
 * the resulting value is made available to all vertices in superstep S + 1.
 */

/*
 * Each worker server will have one and only one Aggregator instance   
 */

public abstract class Aggregator implements Functionable {

    public Computable aggregatedValue;

    @Override
    public void execute(Context context) {

        this.aggregate(context.getVertexGroup());

        Integer serverId = context.getServerId();
        List<Integer> vertexIds = getAllVertexIdsOnOtherWorkers(context.getVertexToServer(), serverId);

        this.broadcast(
                serverId,
                vertexIds,
                context.getOutMailTable(),
                aggregatedValue);
    }

    // how the aggregated value is initialized from the first input value
    public void initialize(Computable initAggregatedValue) {
        aggregatedValue = initAggregatedValue;
    }

    // How multiple partially aggregated values are reduced to one.
    // Aggregation operators should be commutative and associative.
    public abstract void aggregate(VertexGroup vertexGroup);

    private void broadcast(
            Integer serverId,
            List<Integer> vertexIds,
            ConcurrentHashMap<Integer, CopyOnWriteArrayList<Mail>> outMailTable,
            Computable aggregated_value) {

        // TODO: send to all vertices by mails
        // create mails for all and send

        Message message = new Message(aggregated_value, Message.MessageType.BROADCAST);
        Mail newMail = new Mail(serverId, -1, -1, message, MailType.BROADCAST);
        List<Mail> mailList = new ArrayList<>();
        mailList.add(newMail);

        for (int i : vertexIds) {
            outMailTable.put(i, new CopyOnWriteArrayList<>(mailList));
        }
    }

    public abstract void stop();

    private List<Integer> getAllVertexIdsOnOtherWorkers(Map<Integer,Integer> vertexToServerMap, Integer serverId){
        List<Integer> vertexIds = new ArrayList<>();
        for (Map.Entry<Integer,Integer> entry : vertexToServerMap.entrySet()) {
            if (entry.getValue() != serverId){
                vertexIds.add(entry.getKey());
            }
        }
        return vertexIds;
    }

}
