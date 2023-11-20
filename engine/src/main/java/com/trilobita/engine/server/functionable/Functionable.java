package com.trilobita.engine.server.functionable;

import java.util.concurrent.CopyOnWriteArrayList;

import com.trilobita.commons.Mail;
import com.trilobita.engine.server.Context;

/*
 * An abstract class for easy adding or removing functional blocks, 
 * We provide the implementation of Combiner and Aggregator, as discussed in Pregel.
 */
public interface Functionable {
    void execute(Context context, CopyOnWriteArrayList<Mail> mailList);
}
