package com.trilobita.core.graph;


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

    public Vertex getVertexById(int id) {
        for (Vertex vertex: vertexSet){
            if (vertex.getId() == id){
                return vertex;
            }
        }
        return null;
    }
}
