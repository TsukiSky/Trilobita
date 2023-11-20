package com.trilobita.engine.server.functionable.examples.aggregators;

import com.trilobita.engine.server.Context;

import java.util.List;

import com.trilobita.commons.Computable;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.engine.server.functionable.Aggregator;

/*
 * Sum total number of edges in the graph.
 * Applied to the out-degree of each vertex yields
 */
public class MinValueAggregator extends Aggregator {

        private static MinValueAggregator instance;

        public MinValueAggregator(int instanceID, Computable initAggregatedValue) {
                super(instanceID, initAggregatedValue);
        }

        public static synchronized MinValueAggregator getInstance(Context context, int instanceID) {
                if (instance == null) {
                        instance = new MinValueAggregator(instanceID, instance.aggregate(context.getVertexGroup()));
                }
                return instance;
        }

        @Override
        public Computable aggregate(VertexGroup vertexGroup) {
                Computable min_value = null;

                List<Vertex> vertices = vertexGroup.getVertices();
                for (Vertex vertex : vertices) {
                        Computable value = vertex.getValue();
                        if (min_value == null) {
                                min_value = value;
                        } else {
                                if (value.compareTo(min_value) < 0) {
                                        min_value.setValue(value);
                                }
                        }

                }
                return min_value;
        }

        @Override
        public void stop() {
                instance = null;
        }
}
