package com.trilobita.core.graph;

import com.trilobita.core.graph.vertex.Vertex;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
@AllArgsConstructor
public class Graph extends VertexGroup {
    public Graph(List<Vertex> vertices) {
        super();
        this.vertices = vertices;
    }

    public void addVertex(Vertex v){
        this.vertices.add(v);
    }


}
