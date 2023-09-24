package com.trilobita.core.graph.vertex;

import com.trilobita.commons.Mail;
import com.trilobita.commons.Message;
import com.trilobita.commons.MailType;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

@EqualsAndHashCode(callSuper = true)
@Data
public class ChannelVertex extends FunctionalVertex{
    private BlockingQueue<Message> readQueue;
    private BlockingQueue<Message> updateQueue;
    private HashMap<Integer, Set<Integer>> directConnections;
    @Override
    public void function() {
//        forward message
    }

    @Override
    public void onReceive(Mail mail){
//        channel vertex will first check whether the sender and receiver are
//        connected, if they are, it will be added in to the update queue
//        otherwise added to the read queue
        int senderId = mail.getFromVertexId();
        int receiverId = mail.getToVertexId();
        Message msg = mail.getMessage();
        MailType mailType = mail.getMailType();
        if (mailType == MailType.NORMAL){
           if (directConnections.get(senderId).contains(receiverId)){
               updateQueue.add(msg);
           }
           else {
               readQueue.add(msg);
           }
        }
    }
}
