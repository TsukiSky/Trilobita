package com.trilobita.server.functionable.examples;

import java.util.concurrent.BlockingQueue;

import com.trilobita.commons.Mail;
import com.trilobita.server.functionable.Combiner;

/*
 * Sum all messages sent to the same vertex.
 */
public class SumCombiner extends Combiner {

    @Override
    public void combine(BlockingQueue<Mail> outMailQueue) {
        // get all messages to the same vertex
        // sum them all and record
        // send it out
    }
    
}
