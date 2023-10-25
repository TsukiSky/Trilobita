package com.trilobita.engine.computing.task;

import com.trilobita.commons.Message;
import com.trilobita.core.graph.vertex.Vertex;
import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class VertexTask extends Task {
    private Vertex vertex;

    @Override
    public void run() {
        vertex.compute();
    }
}
