package com.trilobita.engine.server.util.functionable.instance.aggregator;

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
        public Integer aggregate(VertexGroup vertexGroup) {
                List<Integer> numEdgesList = new ArrayList<>();
                List<Vertex<?>> vertices = vertexGroup.getVertices();
                for (Vertex<?> vertex : vertices) {
                        Integer numEdges = vertex.getEdges().size();
                        numEdgesList.add(numEdges);
                }
                return this.reduce(numEdgesList);
        }

        @Override
        public Integer reduce(List<Integer> numEdgesList) {
                Integer total_edges = initAggregatedValue;
                for (Integer num_edges : numEdgesList) {
                        total_edges += num_edges;
                }
                return total_edges ;
        }
}
