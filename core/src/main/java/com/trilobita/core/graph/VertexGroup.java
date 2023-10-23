package com.trilobita.core.graph;


import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.exception.TrilobitaException;
import lombok.Data;
import java.util.ArrayList;
import java.util.List;

@Data
public class VertexGroup {
    protected List<Vertex> vertexSet;

    public VertexGroup() {
        this.vertexSet = new ArrayList<>();
    }

    public Vertex getVertexById(int id) throws TrilobitaException {
        for (Vertex vertex: vertexSet){
            if (vertex.getId() == id){
                return vertex;
            }
        }
        throw new TrilobitaException("Vertex Not Found");
    }

    public static void main(String[] args) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonVertexGroup = "{ \"vertexSet\": [ { \"id\": 1}] }";  // Example JSON representation of VertexGroup
        VertexGroup result = objectMapper.readValue(jsonVertexGroup, VertexGroup.class);
    }

}


