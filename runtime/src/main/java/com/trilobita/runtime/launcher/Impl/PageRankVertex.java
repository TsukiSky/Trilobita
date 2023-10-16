package com.trilobita.runtime.launcher.Impl;

import com.trilobita.commons.Computable;
import com.trilobita.commons.Mail;
import com.trilobita.commons.Message;
import com.trilobita.core.graph.vertex.Vertex;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class PageRankVertex extends Vertex<Integer> {
    private final double weight = 0.75;
    PageRankVertex(int id){
        super(id, new IntComparable(1),new ArrayList<>(), false, new LinkedBlockingQueue<>(),
                false, new LinkedBlockingQueue<>());
    }
    @Override
    public void startSuperstep(){
//        initialize the score to be (1-weight) * score in previous superstep
        this.setState(((IntComparable)this.getState()).multiply(1-weight));
    }
    @Override
    public void compute(Message<Integer> message){
        IntComparable score = (IntComparable) message.getContent();
//      update the state of the vertex according to the incoming score
        this.setState(this.getState().add(score.multiply(weight)));
    }
}
