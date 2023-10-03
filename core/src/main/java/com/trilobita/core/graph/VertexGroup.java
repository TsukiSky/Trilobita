package com.trilobita.core.graph;

import com.trilobita.core.graph.vertex.AbstractVertex;
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
    protected List<AbstractVertex> VertexSet;
    public abstract void setVertexSet(List<AbstractVertex> list);
    private List<AbstractVertex> vertexSet;

    public AbstractVertex getVertexById(int id) throws TrilobitaException {
        for (AbstractVertex vertex: vertexSet) {
            if (vertex.getId() == id) {
                return vertex;
            }
        }
        throw new TrilobitaException("Vertex with id " + id + " not found");
    }
}


