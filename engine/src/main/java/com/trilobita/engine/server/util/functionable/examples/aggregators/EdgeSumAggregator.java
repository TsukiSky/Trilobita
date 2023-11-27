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
public class EdgeSumAggregator extends Aggregator<Integer> {
        Computable<Integer> initAggregatedValue;

        public EdgeSumAggregator(Computable<Integer> initAggregatedValue, String topic) {
                super(initAggregatedValue, topic);
                this.initAggregatedValue = initAggregatedValue;
        }

        private static EdgeSumAggregator instance;

        public static synchronized EdgeSumAggregator getInstance(Computable<Integer> initAggregatedValue,
                        String topic) {
                if (instance == null) {
                        instance = new EdgeSumAggregator(initAggregatedValue, topic);
                }
                return instance;
        }

        @Override
        public Computable<Integer> aggregate(VertexGroup vertexGroup) {
                List<Computable<?>> computables = new ArrayList<>();
                List<Vertex<?>> vertices = vertexGroup.getVertices();
                for (Vertex<?> vertex : vertices) {
                        Integer numEdges = vertex.getEdges().size();
                        this.getNewFunctionableValue().setValue(numEdges);
                        computables.add(this.getNewFunctionableValue());
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
                log.info("Total edges calculated by {}: {}", this.getServerId(), total_edges.getValue());
                return total_edges;
        }
}
