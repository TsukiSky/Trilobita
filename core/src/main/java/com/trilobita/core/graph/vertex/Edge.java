package com.trilobita.core.graph.vertex;

import com.trilobita.commons.Computable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Edge {
    private int fromVertexId;
    private int toVertexId;
    private Computable<?> state;
}
