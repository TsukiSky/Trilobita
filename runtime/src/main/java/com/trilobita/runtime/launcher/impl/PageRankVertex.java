package com.trilobita.runtime.launcher.impl;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.trilobita.commons.*;
import com.trilobita.core.graph.vertex.Edge;
import com.trilobita.core.graph.vertex.Vertex;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

@NoArgsConstructor
@Data
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public class PageRankVertex extends Vertex<BigDecimal> {
    private final double weight = 0.85;
    private DoubleComputable state;
    public PageRankVertex(int id){
        super(id, new ArrayList<>(), false, new LinkedBlockingQueue<>(),
                false, new LinkedBlockingQueue<>());
        this.state = new DoubleComputable(BigDecimal.valueOf(1));
    }

    @Override
    public void startSuperstep(){
        // initialize the score to be (1-weight) * score in previous superstep
        this.getState().setValue(BigDecimal.valueOf(1-weight));
    }
    @Override
    public void compute(Message message){
        DoubleComputable score = (DoubleComputable) message.getContent();
        // update the state of the vertex according to the incoming score
        this.setState((DoubleComputable) this.getState().add(score.multiply(weight)));
        // if finish all the job, generate out mail
        if (this.getIncomingQueue().isEmpty()){
        // calculate the updated edge weight
            Message msg = new Message(this.getState().getValue().divide(BigDecimal.valueOf(this.getEdges().size())), MessageType.NORMAL);
            for (Edge edge: this.getEdges()){
                int vertexId = edge.getToVertexId();
                Mail mail = new Mail(vertexId, msg, MailType.NORMAL);
                this.sendMail(mail);
            }
        }
    }
}
