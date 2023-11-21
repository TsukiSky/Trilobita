package com.trilobita.core.graph;


import com.trilobita.commons.Computable;
import com.trilobita.core.graph.vertex.Vertex;
import lombok.Data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

@Data
public class VertexGroup<T> implements Serializable {
    protected List<Vertex<T>> vertices;
    protected int size;

    public VertexGroup() {
        this.vertices = new LinkedList<>();
    }

    /**
     * Get the vertex by id
     * @param id the id of the vertex
     * @return the vertex if found, null otherwise
     */
    public Vertex<T> getVertexById(int id) {
        for (Vertex<T> vertex: vertices){
            if (vertex.getId() == id){
                return vertex;
            }
        }
        return null;
    }

    /**
     * Get the vertex values of the graph
     * @return the vertex values
     */
    public HashMap<Integer, Computable<T>> getVertexValues() {
        HashMap<Integer, Computable<T>> vertexValues = new HashMap<>();
        for (Vertex<T> v : vertices) {
            vertexValues.put(v.getId(), v.getValue());
        }
        return vertexValues;
    }
}
