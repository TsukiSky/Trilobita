package com.trilobita.core.graph.Vertex;


import com.trilobita.commons.Mail;
import com.trilobita.commons.Message;

public abstract class FunctionalVertex extends AbstractVertex {
    public abstract void function();

    public void onReceive(Mail mail){
        Message msg = mail.getMessage();
        this.getIncomingQueue().add(msg);
    };
}
