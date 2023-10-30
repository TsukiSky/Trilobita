package com.trilobita.core.graph;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trilobita.core.graph.vertex.Vertex;
import lombok.Data;
import lombok.Getter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

@Data
public class VertexGroup<T> implements Serializable {
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

}
