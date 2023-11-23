package com.trilobita.engine.server.functionable.examples.aggregators;

import java.util.ArrayList;
import java.util.List;

import com.trilobita.commons.Computable;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.engine.server.functionable.Aggregator;

/*
 * Sum total number of edges in the graph.
 * Applied to the out-degree of each vertex yields
 */
public class MinValueAggregator extends Aggregator<Double> {

        private static MinValueAggregator instance;

        public MinValueAggregator(Computable<Double> initAggregatedValue) {
                super(initAggregatedValue);
        }

        public static synchronized MinValueAggregator getInstance() {
                if (instance == null) {
                        Computable<Double> init = null;
                        init.setValue(Double.POSITIVE_INFINITY);
                        instance = new MinValueAggregator(init);
                }
                return instance;
        }

        @Override
        public Computable<Double> aggregate(VertexGroup vertexGroup) {
                
                List<Computable<Double>> computables = new ArrayList<>();
                List<Vertex<Double>> vertices = vertexGroup.getVertices();
                for (Vertex<Double> vertex : vertices) {
                        computables.add(vertex.getValue());
                }
                Computable<Double> min_value = this.reduce(computables);
                return min_value;
        }

        @Override
        public void stop() {
                instance = null;
        }

        @Override
        public Computable<Double> reduce(List<Computable<Double>> computables) {
                Computable<Double> min_value  = null;
                for (Computable<Double> computable : computables) {
                        Double value = computable.getValue();
                        if (min_value == null) {
                                min_value.setValue(value);
                        } else {
                                if (min_value.compareTo(value) > 0) {
                                        min_value.setValue(value);
                                }
                        }
                }
                return min_value;
        }
}
