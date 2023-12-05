package com.trilobita.examples;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trilobita.core.common.Computable;
import com.trilobita.core.graph.Graph;
import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.examples.shortestpath.vertex.ShortestPathValue;
import com.trilobita.examples.shortestpath.vertex.ShortestPathVertex;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class GraphLoader {
    public  static final ObjectMapper objectMapper = new ObjectMapper();

    public static void store(String outDir, String fileNameStr, Object o) {
        try {
            objectMapper.writeValue(new File(outDir, fileNameStr), o);
            log.info("[JsonParser] JSON file stored");
        } catch (IOException e) {
            log.error("[JsonParser] Failed to store JSON file", e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> Graph<T> loadGraph(String fileDirStr, Class<?> vertexClass, boolean edgeWeightFlag, Class<?> computableClass) throws IOException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Class<Vertex<T>> vertexClazz = (Class<Vertex<T>>) vertexClass;
        Class<Computable<T>> computableClazz = (Class<Computable<T>>) computableClass;

        String content = new String(Files.readAllBytes(Paths.get(fileDirStr)));
        JSONObject jsonObject = new JSONObject(content);
        JSONArray verticesArray = jsonObject.getJSONArray("vertices");
        List<Vertex<T>> vertices = new ArrayList<>();
        for (int i = 0; i < verticesArray.length(); i++) {
            JSONObject vertexObject = verticesArray.getJSONObject(i);
            int id = vertexObject.getInt("id");
            Vertex<T> vertex = vertexClazz.getDeclaredConstructor(int.class).newInstance(id);
            vertex.setStatus(Vertex.VertexStatus.valueOf(vertexObject.getString("status")));
            JSONArray edgesArray = vertexObject.getJSONArray("edges");
            for (int j = 0; j < edgesArray.length(); j++) {
                JSONObject edgeObject = edgesArray.getJSONObject(j);
                int toVertexId = edgeObject.getInt("toVertexId");
                if (edgeWeightFlag) {
                    double edgeValue = edgeObject.getJSONObject("state").getDouble("value");
                    vertex.addEdge(toVertexId, computableClazz.getDeclaredConstructor(double.class).newInstance(edgeValue));
                }else {
                    vertex.addEdge(toVertexId);
                }
            }
            vertices.add(vertex);
        }
        return new Graph<>(vertices);
    }

    public static Graph<Double> loadShortestPathGraph(String filePath) throws IOException {
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
