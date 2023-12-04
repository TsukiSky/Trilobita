package com.trilobita.runtime.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trilobita.commons.Computable;
import com.trilobita.core.graph.Graph;
import com.trilobita.core.graph.vertex.Vertex;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class JsonParser {
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
    public static <T> Graph<T> read(String fileDirStr, Class<?> vertexClass, boolean edgeWeightFlag, Class<?> computableClass) throws IOException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
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
}
