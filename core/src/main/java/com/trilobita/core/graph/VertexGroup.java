package com.trilobita.core.graph;


import com.trilobita.core.graph.vertex.Vertex;
import com.trilobita.exception.TrilobitaException;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public abstract class VertexGroup {
    protected List<Vertex> vertexSet;

    public Vertex getVertexById(int id) throws TrilobitaException {

        for (Vertex vertex: vertexSet){
            if (vertex.getId() == id){
                return vertex;
            }
        }

        throw new TrilobitaException("Vertex Not Found");
    }
}


