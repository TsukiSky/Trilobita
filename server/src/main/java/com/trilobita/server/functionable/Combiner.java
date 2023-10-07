package com.trilobita.server.functionable;

import java.util.concurrent.BlockingQueue;

import com.trilobita.commons.Mail;
import com.trilobita.server.Context;

/*
 * Combine outcoming messages from a workerserver to another workerserver.
 */
public abstract class Combiner implements Functinable{

    @Override
    public void execute(Context context) {
       this.combine(context.getOutMailQueue());
    }

    public abstract void combine(BlockingQueue<Mail> outMailQueue);

}
