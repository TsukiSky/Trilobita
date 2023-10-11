package com.trilobita.core.graph;

import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.core.graph.vertex.Edge;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
@AllArgsConstructor
public class Graph extends VertexGroup {
    public Graph(List<Vertex<?>> vertices) {
        super();
        this.vertexSet = vertices;
    }

    public void addVertex(Vertex<?> v){
        this.vertexSet.add(v);
    }


}
