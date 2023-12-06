package com.trilobita.engine.server.util.functionable.instance.aggregator;

import java.util.ArrayList;
import java.util.List;

import com.trilobita.core.common.Computable;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.engine.server.util.functionable.Aggregator;

import lombok.extern.slf4j.Slf4j;

/*
 * Calculate the min value of all vertices.
 */
@Slf4j
public class MinValueAggregator extends Aggregator<Double> {

        Double initAggregatedValue = Double.POSITIVE_INFINITY;

        public MinValueAggregator(Computable<Double> initLastValue, Computable<Double> initNewValue, String topic) {
                super(initLastValue, initNewValue, topic);
        }

        @Override
        public Double aggregate(VertexGroup vertexGroup) {

                List<Double> values = new ArrayList<>();
                List<Vertex<Double>> vertices = vertexGroup.getVertices();
                for (Vertex<Double> vertex : vertices) {
                        values.add(vertex.getValue().getValue());
                }
                Double min_value = this.reduce(values);
                //log.info("[MinValueAggregator] Min vertex vale calculated by {}: {}", this.getServerId(), min_value);
                return min_value;
        }


        @Override
        public Double reduce(List<Double> values) {
                Double min_value = initAggregatedValue;
                for (Double value : values) {
                        if (min_value > value) {
                                min_value = value;
                        }
                }
                return min_value;
        }
}
