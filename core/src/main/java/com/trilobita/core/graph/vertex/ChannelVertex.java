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
    private BlockingQueue<Mail> readQueue;
    private BlockingQueue<Mail> updateQueue;
    private HashMap<Integer, Set<Integer>> directConnections;
    private HashMap<Integer, Set<Integer>> allConnections;


    /**
     * <p>
     *      Update the hash map for vertex connection
     * </p>
     * @param mail parse from, to vertex id from the mail
     */
    public void updateConnection(Mail mail){
        int senderId = mail.getFromVertexId();
        int receiverId = mail.getToVertexId();
        allConnections.get(senderId).add(receiverId);
        allConnections.get(receiverId).add(senderId);
    }

    @Override
    public void function() {
//        forward message
        if (this.isStepFinish()){
            while (!updateQueue.isEmpty()){
                Mail mail = updateQueue.poll();
                allConnections.get(mail.getFromVertexId()).add(mail.getToVertexId());
            }
            this.sendFinish();
        }
        else {

        }
    }

    @Override
    public void onReceive(Mail mail){
//        channel vertex will first check whether the sender and receiver are
//        connected, if they are, it will be added in to the update queue
//        otherwise added to the read queue
        int senderId = mail.getFromVertexId();
        int receiverId = mail.getToVertexId();
        MailType mailType = mail.getMailType();
        if (mailType == MailType.NORMAL){
           if (directConnections.get(senderId).contains(receiverId)){
               updateQueue.add(mail);
           }
           else {
               if (allConnections.get(senderId).contains(receiverId)){
                   readQueue.add(mail);
               }
           }
        }
    }
}
