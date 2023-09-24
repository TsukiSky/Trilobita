package com.trilobita.core.graph;

import com.trilobita.core.graph.vertex.AbstractVertex;
import com.trilobita.core.graph.vertex.Edge;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

@Data
@AllArgsConstructor
public class Graph extends VertexGroup {
    public void addVertex(AbstractVertex v){
        this.VertexSet.add(v);
    }

    public void addEdge(AbstractVertex v1, AbstractVertex v2){
        Edge edge = new Edge(v1,v2,null);
        List<Edge> list = v1.getEdges();
        list.add(edge);
        v1.setEdges(list);
        List<Edge> list2 = v2.getEdges();
        list2.add(edge);
        v2.setEdges(list2);
    }
}
