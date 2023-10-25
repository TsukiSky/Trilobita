package com.trilobita.core.graph;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trilobita.core.graph.vertex.Vertex;
import lombok.Data;
import java.util.ArrayList;
import java.util.List;

@Data
public class VertexGroup {
    protected List<Vertex> vertices;

    public VertexGroup() {
        this.vertices = new ArrayList<>();
    }

    public Vertex getVertexById(int id) {
        for (Vertex vertex: vertices){
            if (vertex.getId() == id){
                return vertex;
            }
        }
        return null;
    }

    public static void main(String[] args) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonVertexGroup = "{ \"vertexSet\": [ { \"id\": 1}] }";  // Example JSON representation of VertexGroup
        VertexGroup result = objectMapper.readValue(jsonVertexGroup, VertexGroup.class);
    }

}
