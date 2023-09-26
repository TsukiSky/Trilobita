package com.trilobita.core.graph.vertex;

import com.trilobita.commons.Message;

public class CombinerVertex extends FunctionalVertex implements Combiner {
    @Override
    public void function() {

    }

    @Override
    public Message<?> combine(Iterable<?> iterable){
        if (iterable.iterator().hasNext()){
            return null;
        }
        return new Message<>();
    }
}
