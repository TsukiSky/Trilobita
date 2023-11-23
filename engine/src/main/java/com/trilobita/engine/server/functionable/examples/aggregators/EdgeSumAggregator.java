package com.trilobita.engine.server.functionable.examples.aggregators;

import com.trilobita.engine.server.Context;

import java.util.List;

import com.trilobita.commons.Computable;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.core.graph.vertex.Edge;
import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.engine.server.functionable.Aggregator;
import com.trilobita.core.messaging.MessageConsumer.MessageHandler;

/*
 * Sum total number of edges in the graph.
 * Applied to the out-degree of each vertex yields
 */
public class EdgeSumAggregator extends Aggregator<Integer> {

        public EdgeSumAggregator(Computable<Integer> initAggregatedValue) {
                super(initAggregatedValue);
        }

        private static EdgeSumAggregator instance;

        public static synchronized EdgeSumAggregator getInstance(Context context) {
                if (instance == null) {
                        instance = new EdgeSumAggregator(instance.aggregate(context.getVertexGroup()));
                }
                return instance;
        }

        @Override
        public Computable<Integer> aggregate(VertexGroup vertexGroup) {
                List<Computable<Integer>> computables = new List<Computable<?>>();
                List<Vertex<?>> vertices = vertexGroup.getVertices();
                for (Vertex<?> vertex : vertices) {
                        Integer numEdges = (Integer) vertex.getEdges().size();
                        computables.add(numEdges);
                }
                return this.reduce(computables);
        }

        @Override
        public void stop() {
                instance = null;
        }

        @Override
        public Computable<Integer> reduce(List<Computable<?>> computables) {
                Computable<Integer> total_edges = this.initAggregatedValue;
                for (Computable<?> edge : computables) {
                        total_edges.add((Computable<Integer>) edge);
                }
                return total_edges;
        }
}
