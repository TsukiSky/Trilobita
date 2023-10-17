package com.trilobita.runtime.launcher.impl;

import com.trilobita.commons.Message;
import com.trilobita.core.graph.vertex.Vertex;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

@NoArgsConstructor
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
    }
    @Override
    public void compute(Message<BigDecimal> message){
        DoubleComputable score = (DoubleComputable) message.getContent();
//      update the state of the vertex according to the incoming score
        this.setState(this.getState().add(score.multiply(weight)));
    }
}
