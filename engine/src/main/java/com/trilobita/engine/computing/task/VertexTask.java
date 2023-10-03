package com.trilobita.engine.computing.task;

import com.trilobita.core.graph.vertex.AbstractVertex;
import com.trilobita.core.graph.vertex.NormalVertex;

public class VertexTask extends Task {
    private NormalVertex vertex;

    @Override
    public void run() {
        vertex.compute();
    }
}
