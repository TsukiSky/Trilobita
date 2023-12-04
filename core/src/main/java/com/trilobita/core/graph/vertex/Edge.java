package com.trilobita.core.graph.vertex;

import com.trilobita.core.common.Computable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Edge implements Serializable {
    private int fromVertexId;
    private int toVertexId;
    private Computable<?> state;
}
