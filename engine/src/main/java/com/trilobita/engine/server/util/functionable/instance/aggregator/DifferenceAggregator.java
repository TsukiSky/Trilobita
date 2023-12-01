package com.trilobita.engine.server.util.functionable.instance.aggregator;

import java.util.List;

import com.trilobita.commons.Computable;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.engine.server.util.functionable.Aggregator;

import lombok.extern.slf4j.Slf4j;

/*
 * Detect convergence condition of all vertices.
 */
@Slf4j
public class DifferenceAggregator extends Aggregator<Double> {

    static Double initAggregatedValue = 0.0;
    static Double tolerance = 0.00001;

    public DifferenceAggregator(Computable<Double> initLastValue, Computable<Double> initNewValue, String topic) {
        super(initLastValue, initNewValue, topic);
    }

    /**
     * Calculate a global difference for all vertex values
     *
     * @return difference between old and new vertex values
     */
    @Override
    public Double aggregate(VertexGroup vertexGroup) {
        Double totalDiff = initAggregatedValue;
        List<Vertex<Double>> vertices = vertexGroup.getVertices();
        for (Vertex<Double> vertex : vertices) {
            totalDiff += Math.abs(vertex.getValue().minus(vertex.getValueLastSuperstep()).getValue());
        }
//        log.info("Difference of all vertex values calculated by {}: {}", this.getServerId(), min_value.getValue());
        return totalDiff;
    }


    /**
     * Reduce on the master server side.
     *
     * @param values difference on each server aggregated by workers
     */
    @Override
    public Double reduce(List<Double> values) {
        Double totalDiff = initAggregatedValue;
        for (Double value : values) {
            totalDiff += value;
        }
        if (totalDiff < tolerance) {
            //TODO: CONVERGENCE REACHED
        }
        return totalDiff;
    }
}
