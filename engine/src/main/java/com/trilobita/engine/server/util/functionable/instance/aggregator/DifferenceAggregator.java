package com.trilobita.engine.server.util.functionable.instance.aggregator;

import java.util.List;

import com.trilobita.core.common.Computable;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.engine.server.AbstractServer;
import com.trilobita.engine.server.util.functionable.Aggregator;
import lombok.extern.slf4j.Slf4j;

/*
 * Detect convergence condition of all vertices.
 */
@Slf4j
public class DifferenceAggregator extends Aggregator<Double> {

    static Double initAggregatedValue = 0.0;
    static Double tolerance = 0.00001;
    int superstep = 0;

    public DifferenceAggregator(Computable<Double> initLastValue, Computable<Double> initNewValue, String topic) {
        super(initLastValue, initNewValue, topic);
    }

    /**
     * Override the original execute to make it possible to access vertex parameters
     *
     * @param server worker server
     */
    public void execute(AbstractServer<?> server) {
        VertexGroup<?> vertexGroup = server.getVertexGroup();
        Double reducedValue = this.aggregate(vertexGroup);
        this.checkTermination(server);
        this.getNewFunctionableValue().setValue(reducedValue);
        superstep++;
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
        // aggregate
        for (Vertex<Double> vertex : vertices) {
            if (vertex.getValueLastSuperstep() != null) {
                totalDiff += Math.abs(vertex.getValue().getValue() - vertex.getValueLastSuperstep().getValue());
            }
        }
        log.info("Difference of all vertex values calculated by {}: {}", this.getServerId(), totalDiff);
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
            totalDiff = 0.0;
        }
        log.info("[DifferenceAggregator] Total difference: {}", totalDiff);
        return totalDiff;
    }

    /**
     * Check whether the total vertex value change from last step < tolerance. if so, mark all vertex's shouldStop as true.
     *
     * @param server worker server
     */
    private void checkTermination(AbstractServer<?> server) {
        // check termination
        if (this.superstep > 0) {
            log.info("[DifferenceAggregator] Checking termination: {}", (this.getLastFunctionableValue().getValue() < tolerance));
            List<? extends Vertex<?>> vertices = server.getVertexGroup().getVertices();
            if (this.getLastFunctionableValue().getValue() < tolerance) {

                for (Vertex<?> vertex : vertices) {
                    vertex.setShouldStop(true);
                }
            }
        }
    }
}
