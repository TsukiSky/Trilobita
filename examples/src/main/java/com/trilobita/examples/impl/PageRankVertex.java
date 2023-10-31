package com.trilobita.examples.impl;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.trilobita.commons.*;
import com.trilobita.commons.Computable;
import com.trilobita.core.graph.vertex.Edge;
import com.trilobita.core.graph.vertex.Vertex;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
public class PageRankVertex extends Vertex<Double> implements Serializable {
    private final double weight = 0.85;

    public PageRankVertex(int id) {
        super(id, new ArrayList<>(), VertexStatus.INACTIVE, new PageRankValue((double) 0), false,
                new LinkedBlockingQueue<>(), null, null);
    }

    @Override
    public void startSuperstep() {
        // initialize the score to be (1-weight) * score in previous superstep
        this.getValue().setValue(1 - weight);
    }

    @Override
    public void compute() {
        startSuperstep();
        while (!this.getIncomingQueue().isEmpty()) {
            Message message = this.getIncomingQueue().poll().getMessage();
            PageRankValue score = (PageRankValue) message.getContent();
            // update the state of the vertex according to the incoming score
            this.getValue().add(score.multiply(weight));
        }
        // finished all the job, generate out mail
        Message msg = new Message(new PageRankValue(this.getValue().getValue()/this.getEdges().size()), Message.MessageType.NORMAL);
        for (Edge edge : this.getEdges()) {
            int vertexId = edge.getToVertexId();
            Mail mail = new Mail(vertexId, msg, Mail.MailType.NORMAL);
            this.sendMail(mail);
        }
        updateServerTempValue();
    }
}
