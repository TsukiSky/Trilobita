package com.trilobita.engine.server.util.functionable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.trilobita.commons.Mail;
import com.trilobita.engine.server.Context;

/*
 * Combine outcoming messages from a workerserver to another workerserver.
 */
public abstract class Combiner implements Functionable {
    @Override
    public void execute(Context context) {
       this.combine(context.getOutMailTable());
    }

    // main method for combine
    public abstract void combine(ConcurrentHashMap<Integer, CopyOnWriteArrayList<Mail>> outMailTable);
}
