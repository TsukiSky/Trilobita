package com.trilobita.engine.server.functionable;

import com.trilobita.engine.server.Context;

/*
 * An interface for easy adding or removing functional blocks, 
 * We provide the implementation of Combiner and Aggregator, as discussed in Pregel.
 * All defined execute functions will be called at the start of a superstep.  
 */
public interface Functinable {

    void execute(Context contxt);

}