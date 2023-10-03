package com.trilobita.core.graph.vertex;


import com.trilobita.commons.Mail;

public abstract class FunctionalVertex extends Vertex {
    public abstract void function();

    public void onReceive(Mail mail){
        this.getIncomingQueue().add(mail);
    }

}
