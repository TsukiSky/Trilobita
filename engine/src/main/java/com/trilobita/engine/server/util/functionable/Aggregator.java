package com.trilobita.engine.server.util.functionable;


import com.trilobita.core.graph.VertexGroup;
import com.trilobita.engine.server.Context;

/*
 * Monitor and communicate vertices metadata on a workerserver.
 */
public abstract class Aggregator implements Functionable {
    @Override
    public void execute(Context context) {
        this.aggregate(context.getVertexGroup());
    }
    public abstract void aggregate(VertexGroup vertexGroup);
}
