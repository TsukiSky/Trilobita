package com.trilobita.engine.server.functionable.examples;

import com.trilobita.engine.server.Context;

import java.util.List;

import com.trilobita.commons.Computable;
import com.trilobita.core.graph.VertexGroup;
import com.trilobita.core.graph.vertex.Edge;
import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.engine.server.functionable.Aggregator;

/*
 * Sum total number of edges in the graph.
 * Applied to the out-degree of each vertex yields
 */
public class EdgeSumAggregator extends Aggregator {

        private static EdgeSumAggregator instance;

        public static synchronized EdgeSumAggregator getInstance(Context context) {
                if (instance == null) {
                        instance = new EdgeSumAggregator();
                        instance.initialize(instance.getEdgeSum(context.getVertexGroup()));
                }
                return instance;
        }

        @Override
        public void aggregate(VertexGroup vertexGroup) {
                aggregatedValue = instance.getEdgeSum(vertexGroup);
        }

        private Computable getEdgeSum(VertexGroup vertexGroup) {
                Computable<Integer> total_edges = null;
                total_edges.setValue(0);

                List<Vertex> vertices = vertexGroup.getVertices();
                for (Vertex vertex : vertices) {
                        List<Edge> edges = vertex.getEdges();
                        for (Edge edge : edges) {
                                if (edge.getFromVertexId() == vertex.getId()) {
                                        total_edges.setValue(total_edges.getValue()+1);
                                }
                        }
                }
                return total_edges;
        }

        @Override
        public void stop() {
                instance = null;
        }
}
