package com.trilobita.core.graph;


import com.trilobita.core.graph.vertex.Vertex;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public abstract class VertexGroup {
    protected List<Vertex> VertexSet;
}


