package com.trilobita.examples.shortestpath;

import com.trilobita.core.graph.Graph;
import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.examples.shortestpath.vertex.ShortestPathValue;
import com.trilobita.examples.shortestpath.vertex.ShortestPathVertex;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GraphLoader {
    public static Graph<Double> loadGraph(String filePath) throws IOException {
        List<Vertex<Double>> vertices = new ArrayList<>();
        Map<Integer, ShortestPathVertex> vertexMap = new HashMap<>();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                int fromId = Integer.parseInt(values[0]);
                int toId = Integer.parseInt(values[1]);
                double weight = Math.abs(Double.parseDouble(values[2])); // 取绝对值

                ShortestPathVertex fromVertex;
                if (!vertexMap.containsKey(fromId)) {
                    if (fromId == 1) {
                        fromVertex = new ShortestPathVertex(fromId, 0.0, true);
                        fromVertex.setStatus(Vertex.VertexStatus.ACTIVE);
                    } else {
                        fromVertex = new ShortestPathVertex(fromId, Double.MAX_VALUE, false);
                        fromVertex.setStatus(Vertex.VertexStatus.INACTIVE);
                    }
                    vertexMap.put(fromId, fromVertex);
                    vertices.add(fromVertex);
                } else {
                    fromVertex = vertexMap.get(fromId);
                }

                fromVertex.addEdge(toId, new ShortestPathValue(weight));
            }
        }

        return new Graph<>(vertices);
    }

}
