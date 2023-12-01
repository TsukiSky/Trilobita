package com.trilobita.engine.server.util.functionable.examples.aggregators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.trilobita.commons.Computable;
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
        public Computable<Double> aggregate(VertexGroup vertexGroup) {

                List<Computable<?>> computables = new ArrayList<>();
                List<Vertex<Double>> vertices = vertexGroup.getVertices();
                for (Vertex<Double> vertex : vertices) {
                        computables.add(vertex.getValue());
                }
                Computable<Double> min_value = this.reduce(computables);
                log.info("[MinValueAggregator] Min vertex vale calculated by {}: {}", this.getServerId(), min_value.getValue());
                return min_value;
        }


        @Override
        public Computable<Double> reduce(List<Computable<?>> computables) {
                Double min_value = initAggregatedValue;
                for (Computable<?> computable : computables) {
                        Double value = (Double) computable.getValue();
                        if (min_value > value) {
                                min_value = value;
                        }
                }
                this.getNewFunctionableValue().setValue(min_value);
                return this.getNewFunctionableValue();
        }
}
