package com.trilobita.engine.server.util.functionable;

import com.trilobita.core.common.Computable;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.engine.server.AbstractServer;

import java.util.List;

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

    // how the aggregated value is initialized from the first input value
    public Aggregator(Computable<T> initLastValue, Computable<T> initNewValue, String topicName) {
        super(initLastValue,initNewValue,topicName);
    }

    @Override
    public void execute(AbstractServer<?> server) {
        VertexGroup<?> vertexGroup = server.getVertexGroup();
        T reducedValue = this.aggregate(vertexGroup);
        this.getNewFunctionableValue().setValue(reducedValue);
    }

    @Override
    public void execute(List<Computable<?>> computables) {
        List<T> values = computables.stream()
                .map(computable -> (T)computable.getValue())
                .toList();
        T reducedValue = this.reduce(values);
        this.getNewFunctionableValue().setValue(reducedValue);
    }

    // Retreive certain properties to reduce
    // Make use of reduce function
    public abstract T aggregate(VertexGroup<?> vertexGroup);

    public abstract T reduce(List<T> computables);

}
