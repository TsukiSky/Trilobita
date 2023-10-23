package com.trilobita.core.graph.vertex;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.trilobita.commons.Computable;
import com.trilobita.commons.Mail;
import com.trilobita.commons.MailType;
import com.trilobita.commons.MessageType;
import com.trilobita.commons.Message;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public class Vertex<T> {
    private int id;
    private Computable<T> state;
    private List<Edge> edges;
    private boolean flag;
    @JsonDeserialize(as = LinkedBlockingQueue.class)
    @Getter
    private BlockingQueue<Mail> incomingQueue;
    private boolean stepFinish;
    @JsonDeserialize(as = LinkedBlockingQueue.class)
    private BlockingQueue<Mail> serverQueue;


    /**
     * <p>
     *     Send the finish signal to the server
     * </p>
     */
    public void sendFinish(){
//        tell the server that the vertex has finished its job
        Mail mail = new Mail(-1,-1,null, MailType.FINISH_INDICATOR);
        this.serverQueue.add(mail);
    }

    /**
     * <p>
     *     Push the mail to the server's queue to be sent
     *     to the destination vertex
     * </p>
     * @param mail contains from, to index and message
     */
    public void sendMail(Mail mail){
        this.serverQueue.add(mail);
    }

    /**
     * <p>
     *     send message to neighbors with the sendMail function
     * </p>
     * @param edge to embed the id of the destination node to a mail
     * @param message the message to be sent
     */
    public void sendToNeighbor(Edge edge, Message message){
        Mail mail = new Mail(this.id, edge.getToVertexId(), message, MailType.NORMAL);
        sendMail(mail);
    }

    /**
     * <p>
     *     send message to neighbors with the sendMail function
     * </p>
     * @param to the id of the destination vertex
     * @param message the message to be sent
     */
    public void sendTo(int to, Message message){
        Mail mail = new Mail(this.id, to, message, MailType.NORMAL);
        sendMail(mail);
    }

    public void startSuperstep(){

    }

    /**
     * Add the mail to the incoming queue of the vertex
     * @param mail
     */
    public void onReceive(Mail mail){
        incomingQueue.add(mail);
    };


    public void process(){
        List<Message> processMessages = new ArrayList<>();
        while (!this.getIncomingQueue().isEmpty()){
//            process the message until it reaches the barrier message
            Mail mail = this.getIncomingQueue().poll();
            Message message = mail.getMessage();
            if (message.getMessageType() == MessageType.BARRIER){
                break;
            }
            compute(message);
        }
    }

    public void addEdge(int to){
        Edge edge = new Edge(this.id,to,null);
        this.getEdges().add(edge);
    }

    /**
     * <p>
     *     Compute the updated state
     * </p>
     * @param message a message used for computing the new state
     */
    public void compute(Message message){

    }
}
