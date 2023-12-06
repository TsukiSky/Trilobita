package com.trilobita.examples.shortestpath.vertex;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.trilobita.core.common.Computable;
import com.trilobita.core.common.Mail;
import com.trilobita.core.common.Message;
import com.trilobita.core.graph.vertex.Edge;
import com.trilobita.core.graph.vertex.Vertex;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
@EqualsAndHashCode(callSuper = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
public class ShortestPathVertex extends Vertex<Double> implements Serializable {

    private boolean source;

    public ShortestPathVertex(int id) {
        super(id, new ShortestPathValue(Double.MAX_VALUE));
        this.source = false;
        this.setValueLastSuperstep(new ShortestPathValue(Double.MAX_VALUE));
    }

    public ShortestPathVertex(int id, Double value, Boolean source) {
        super(id, new ShortestPathValue(value));
        this.source = source;
        this.setValueLastSuperstep(new ShortestPathValue(Double.MAX_VALUE));
    }

    @Override
    public void startSuperstep() {
        this.getValue().setValue((double) 0);
        this.sendMail();
    }

    @Override
    public void compute() {
//        startSuperstep();
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        this.getValueLastSuperstep().setValue(this.getValue().getValue());
        if (source) {
            this.sendMail();
            this.source = false;
        }
        List<Double> allvalue = new ArrayList<>();
        while (!this.getIncomingQueue().isEmpty()) {
            Message message = this.getIncomingQueue().poll().getMessage();
            ShortestPathValue score = (ShortestPathValue) message.getContent();
            allvalue.add(score.getValue());
        }
        Double minvalue = Double.MAX_VALUE;
        if (!allvalue.isEmpty()) {
            minvalue = Collections.min(allvalue);
            //log.info("[COMPUTE] Min value received: {}", minvalue);
        } else {
            //log.info("[COMPUTE] The list is empty");
        }
        if (minvalue < this.getValue().getValue()) {
            //log.info("[COMPUTE] Min value received: {} ", minvalue);
            //log.info("[COMPUTE] Current value: {}", this.getValue().getValue());
            this.getValue().setValue(minvalue);
            this.sendMail();
        }
    }

    @Override
    public boolean checkStop(Computable<Double> c) {
        return false;
    }

    @Override
    public void sendMail() {
        // finished all the job, generate out mail
        for (Edge edge : this.getEdges()) {
            //log.info("the vertex value {} and edge value {}", this.getValue().getValue(), (double) edge.getState().getValue());
            ShortestPathValue shortestPathValue = new ShortestPathValue(this.getValue().getValue() + (double) edge.getState().getValue());
            Message msg = new Message(shortestPathValue);
            int vertexId = edge.getToVertexId();
            Mail mail = new Mail(vertexId, msg, Mail.MailType.NORMAL);
            this.addMailToServerQueue(mail);
        }
    }
}
