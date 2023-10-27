package com.trilobita.core.graph;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trilobita.core.graph.vertex.Vertex;
import lombok.Data;
import lombok.Getter;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

@Data
public class VertexGroup<T> {
    protected List<Vertex<T>> vertices;

    public VertexGroup() {
        this.vertices = new LinkedList<>();
    }

    public Vertex<T> getVertexById(int id) {
        for (Vertex<T> vertex: vertices){
            if (vertex.getId() == id){
                return vertex;
            }
        }
        return null;
    }

    //    public static void main(String[] args) throws JsonProcessingException {
//        ObjectMapper objectMapper = new ObjectMapper();
//        String jsonVertexGroup = "{ \"vertexSet\": [ { \"id\": 1}] }";  // Example JSON representation of VertexGroup
//        VertexGroup<T> result = objectMapper.readValue(jsonVertexGroup, VertexGroup.class);
//    }
}
