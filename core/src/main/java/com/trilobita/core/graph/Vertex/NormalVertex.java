package com.trilobita.core.graph.Vertex;


import com.trilobita.commons.Message;
import com.trilobita.commons.MessageType;

import java.util.ArrayList;
import java.util.List;

public class NormalVertex extends AbstractVertex{
    public void process(){
        List<Message> messages = new ArrayList<>();
        while (!this.getIncomingQueue().isEmpty()){
//            process the message until it reaches the barrier message
            Message m = this.getIncomingQueue().poll();
            if (m.getMessageType() == MessageType.BARRIER){
                break;
            }
            messages.add(m);
        }
        compute(messages);
    }
    public void compute(List<Message> messages){
//        update the state according to the incoming messages

    }
}
