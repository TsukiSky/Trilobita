package com.trilobita.engine.computing.task;

import com.trilobita.core.graph.vertex.Vertex;

public class VertexTask extends Task {
    private Vertex vertex;

    @Override
    public void run() {
        vertex.process();
    }
}
