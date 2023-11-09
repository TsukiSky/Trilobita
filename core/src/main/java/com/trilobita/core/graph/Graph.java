package com.trilobita.core.graph;

import com.trilobita.core.graph.vertex.Vertex;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
@AllArgsConstructor
public class Graph<T> extends VertexGroup<T> {
    public Graph(List<Vertex<T>> vertices) {
        super();
        this.vertices = vertices;
        this.size = vertices.size();
    }

    public void addVertex(Vertex<T> v){
        this.vertices.add(v);
    }
}
