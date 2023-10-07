package com.trilobita.core.graph;

import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.core.graph.vertex.Edge;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
@AllArgsConstructor
public class Graph extends VertexGroup {
    public Graph(List<Vertex> vertices) {
        super();
        this.vertexSet = vertices;
    }

    public void addVertex(Vertex v){
        this.vertexSet.add(v);
    }

    public void addEdge(Vertex v1, Vertex v2){
        Edge edge = new Edge(v1,v2,null);
        List<Edge> list = v1.getEdges();
        list.add(edge);
        v1.setEdges(list);
        List<Edge> list2 = v2.getEdges();
        list2.add(edge);
        v2.setEdges(list2);
    }
}
