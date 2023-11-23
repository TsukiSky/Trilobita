package com.trilobita.engine.server.functionable;

import java.util.List;

import com.trilobita.commons.Computable;
import com.trilobita.core.graph.VertexGroup;

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
    public Computable<T> initAggregatedValue;
    // how the aggregated value is initialized from the first input value
    public Aggregator(Computable<T> initAggregatedValue) {
        super();
        this.initAggregatedValue = initAggregatedValue;
        this.setNewFunctionableValue(initAggregatedValue);
    }

    @Override
    public void execute(Object object) {
        VertexGroup<?> vertexGroup = (VertexGroup<?>) object;
        this.setNewFunctionableValue(this.aggregate(vertexGroup));
    }

    @Override
    public void execute(List<Computable<?>> computables) {
        this.setNewFunctionableValue(this.reduce(computables));
    }

    // Retreive certain properties to reduce
    // Make use of reduce function
    public abstract Computable<T> aggregate(VertexGroup<?> vertexGroup);

    public abstract Computable<T> reduce(List<Computable<?>> computables);

    public abstract void stop();

}
