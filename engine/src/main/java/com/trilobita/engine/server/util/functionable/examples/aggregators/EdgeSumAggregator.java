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
        Integer initAggregatedValue = 0;
        public EdgeSumAggregator(Computable<Integer> initLastValue, Computable<Integer> initNewValue, String topic) {
                super(initLastValue, initNewValue, topic);
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
        public Computable<Integer> reduce(List<Computable<?>> computables) {
                Integer total_edges = initAggregatedValue;
                for (Computable<?> edge : computables) {
                        Integer num_edges = (Integer) edge.getValue();
                        total_edges += num_edges;
                }
                this.getNewFunctionableValue().setValue(total_edges);
                return this.getNewFunctionableValue();
        }
}
