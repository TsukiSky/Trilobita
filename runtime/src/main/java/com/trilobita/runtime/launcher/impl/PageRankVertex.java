package com.trilobita.runtime.launcher.impl;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.trilobita.commons.Computable;
import com.trilobita.commons.Message;
import com.trilobita.core.graph.vertex.Vertex;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

@NoArgsConstructor
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public class PageRankVertex extends Vertex<BigDecimal> {
    private final double weight = 0.75;
    public PageRankVertex(int id){
        super(id, new DoubleComputable(BigDecimal.valueOf(1)),new ArrayList<>(), false, new LinkedBlockingQueue<>(),
                false, new LinkedBlockingQueue<>());
    }

    @Override
    public void startSuperstep(){
//        initialize the score to be (1-weight) * score in previous superstep
        this.setState(((DoubleComputable)this.getState()).multiply(1-weight));
        System.out.println(this.getState());

    }
    @Override
    public void compute(Message message){
        DoubleComputable score = (DoubleComputable) message.getContent();
//      update the state of the vertex according to the incoming score
        this.setState(this.getState().add(score.multiply(weight)));
    }
}
