package com.trilobita.engine.server.functionable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.trilobita.commons.Mail;
import com.trilobita.core.graph.vertex.utils.Sender;
import com.trilobita.engine.server.Context;

/*
 * Combine outcoming messages from a workerserver to another workerserver.
 */
public abstract class Combiner<T> implements Functinable{

    private Sender sender;

    @Override
    public void execute(Context context) {
       this.combine(context.getOutMailTable(), this.sender);
    }

    // main method for combine
    public abstract void combine(ConcurrentHashMap<Integer, CopyOnWriteArrayList<Mail>> outMailTable, Sender sender); 

}
