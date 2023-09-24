package com.trilobita.core.graph.Vertex;

import com.trilobita.commons.Message;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.concurrent.BlockingQueue;

@Data
@AllArgsConstructor
@NoArgsConstructor
public abstract class AbstractVertex {
    private int id;
    private Value state;
    private List<Edge> edges;
    private BlockingQueue<Message> incomingQueue;
    abstract void sendTo(Message msg, int vertexId);
    abstract void onReceive(Message msg);

}
