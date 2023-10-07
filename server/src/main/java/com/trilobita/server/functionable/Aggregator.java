package com.trilobita.server.functionable;

import com.trilobita.core.graph.VertexGroup;
import com.trilobita.server.Context;

/*
 * Monitor and communicate vertices metadata on a workerserver.
 */
public abstract class Aggregator implements Functinable{

    @Override
    public void execute(Context context) {
        this.aggregate(context.getVertexGroup());
    }

    public abstract void aggregate(VertexGroup vertexGroup);
    
}
