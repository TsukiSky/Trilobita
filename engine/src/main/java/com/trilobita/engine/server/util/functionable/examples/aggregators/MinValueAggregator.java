package com.trilobita.engine.server.util.functionable.examples.aggregators;

import java.util.ArrayList;
import java.util.List;

import com.trilobita.commons.Computable;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.engine.server.util.functionable.Aggregator;

import lombok.extern.slf4j.Slf4j;

/*
 * Sum total number of edges in the graph.
 * Applied to the out-degree of each vertex yields
 */
@Slf4j
public class MinValueAggregator extends Aggregator<Double> {

        private static MinValueAggregator instance;

        public MinValueAggregator(Computable<Double> initValue, String topic) {
                super(initValue, topic);
        }

        public static synchronized MinValueAggregator getInstance(Computable<Double> initValue, String topic) {
                if (instance == null) {
                        instance = new MinValueAggregator(initValue, topic);
                }
                return instance;
        }

        @Override
        public Computable<Double> aggregate(VertexGroup vertexGroup) {

                List<Computable<?>> computables = new ArrayList<>();
                List<Vertex<Double>> vertices = vertexGroup.getVertices();
                for (Vertex<Double> vertex : vertices) {
                        computables.add(vertex.getValue());
                }
                Computable<Double> min_value = this.reduce(computables);
                log.info("Min vertex value calculated by {}: {}", this.getServerId(), min_value.getValue());
                return min_value;
        }

        @Override
        public void stop() {
                instance = null;
        }

        @Override
        public Computable<Double> reduce(List<Computable<?>> computables) {

                for (Computable<?> computable : computables) {
                        Double value = (Double) computable.getValue();
                        if (this.getNewFunctionableValue() == null) {
                                this.getNewFunctionableValue().setValue(value);
                        } else {
                                if (this.getNewFunctionableValue().compareTo(value) > 0) {
                                        this.getNewFunctionableValue().setValue(value);
                                }
                        }
                }
                return this.getNewFunctionableValue();
        }
}
