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

public abstract class Aggregator<T> extends Functionable<T> {

    public int instanceID;

    // how the aggregated value is initialized from the first input value
    public Aggregator(int instanceID, Computable<T> initAggregatedValue) {
        this.instanceID = instanceID;
        this.setFunctionableValue(initAggregatedValue); 
    }

    @Override
    public void execute(Context context) {
        this.setFunctionableValue(this.aggregate(context.getVertexGroup()));
        Message message = new Message(this.getFunctionableValue());
        Mail mail = new Mail(context.getServerId(),-1,message,Mail.MailType.FUNCTIONAL);
        this.sendMail(mail);
    }
    // Retreive certain properties to reduce
    // Make use of reduce function
    public abstract Computable<T> aggregate(VertexGroup<?> vertexGroup);

    public abstract void stop();

}

