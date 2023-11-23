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
public class EdgeSumAggregator extends Aggregator<Integer> {

        public EdgeSumAggregator(Computable<Integer> initAggregatedValue) {
                super(initAggregatedValue);
        }

        private static EdgeSumAggregator instance;

        public static synchronized EdgeSumAggregator getInstance() {
                if (instance == null) {
                        Computable<Integer> init = null;
                        init.setValue(0);
                        instance = new EdgeSumAggregator(init);
                }
                return instance;
        }

        @Override
        public Computable<Integer> aggregate(VertexGroup vertexGroup) {
                List<Computable<Integer>> computables = new ArrayList<Computable<Integer>>();
                List<Vertex<?>> vertices = vertexGroup.getVertices();
                for (Vertex<?> vertex : vertices) {
                        Computable<Integer> com = null;
                        Integer numEdges = vertex.getEdges().size();
                        com.setValue(numEdges);
                        computables.add(com);
                }
                return this.reduce(computables);
        }

        @Override
        public void stop() {
                instance = null;
        }

        @Override
        public Computable<Integer> reduce(List<Computable<Integer>> computables) {
                Computable<Integer> total_edges = this.initAggregatedValue;
                for (Computable<?> edge : computables) {
                        total_edges.add((Computable<Integer>) edge);
                }
                return total_edges;
        }
}
