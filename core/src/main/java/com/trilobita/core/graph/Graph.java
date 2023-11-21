package com.trilobita.core.graph;

import com.trilobita.commons.Computable;
import com.trilobita.core.graph.vertex.Vertex;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The Graph class
 *
 * @param <T>
 */
@EqualsAndHashCode(callSuper = true)
@Data
@AllArgsConstructor
public class Graph<T> extends VertexGroup<T> {
    public Graph(List<Vertex<T>> vertices) {
        super();
        this.vertices = vertices;
        this.size = vertices.size();
    }

    public void addVertex(Vertex<T> v) {
        this.vertices.add(v);
    }

    /**
     * Update the values of the vertices
     *
     * @param vertexValues the new values of the vertices
     */
    public void updateVertexValues(HashMap<Integer, Computable<T>> vertexValues) {
        for (Map.Entry<Integer, Computable<T>> vertex : vertexValues.entrySet()) {
            updateVertexValue(vertex.getKey(), vertex.getValue());
        }
    }

    /**
     * Update the value of a vertex
     *
     * @param id    the id of the vertex
     * @param value the new value of the vertex
     */
    public void updateVertexValue(int id, Computable<T> value) {
        for (Vertex<T> vertex : vertices) {
            if (vertex.getId() == id) {
                vertex.setValue(value);
                break;
            }
        }
    }
}
