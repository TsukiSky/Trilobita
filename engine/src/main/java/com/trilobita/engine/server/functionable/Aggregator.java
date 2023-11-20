package com.trilobita.engine.server.functionable;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.trilobita.commons.Computable;
import com.trilobita.commons.Mail;
import com.trilobita.commons.Message;
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
    public int instanceID;

    // how the aggregated value is initialized from the first input value
    public Aggregator(int instanceID, Computable initAggregatedValue) {
        this.instanceID = instanceID;
        this.aggregatedValue = initAggregatedValue;
    }

    @Override
    public void execute(Context context, CopyOnWriteArrayList<Mail> mailList) {
        this.aggregatedValue = this.aggregate(context.getVertexGroup());
        Message message = new Message(this.aggregatedValue);
        mailList.add(new Mail(context.getServerId(),-1,message,Mail.MailType.BROADCAST));
    }
    // Retreive certain properties to reduce
    // Make use of reduce function
    public abstract Computable aggregate(VertexGroup vertexGroup);

    public abstract void stop();

}

