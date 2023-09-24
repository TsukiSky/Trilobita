package com.trilobita.core.graph.Vertex;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Edge {
    private AbstractVertex from;
    private AbstractVertex to;
    private Value state;
}
