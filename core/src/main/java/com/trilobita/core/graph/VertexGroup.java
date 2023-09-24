package com.trilobita.core.graph;


import com.trilobita.core.graph.Vertex.AbstractVertex;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public abstract class VertexGroup {
    protected List<AbstractVertex> VertexSet;
}


