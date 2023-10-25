package com.trilobita.core.graph.vertex;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.trilobita.commons.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public abstract class Vertex<T> {
    private int id;
    private List<Edge> edges;
    private VertexStatus status;
    private Computable<T> value;
    private boolean stepFinished;
    @JsonDeserialize(as = LinkedBlockingQueue.class)
    private BlockingQueue<Mail> incomingQueue;
    @JsonDeserialize(as = LinkedBlockingQueue.class)
    private BlockingQueue<Mail> serverQueue;

    /**
     * Send the finish signal to the server
     */
    public void sendFinish(){
    // tell the server that the vertex has finished its job
        Mail mail = new Mail(-1,-1,null, MailType.FINISH_INDICATOR);
        this.serverQueue.add(mail);
    }

    /**
     * Push the mail to the server's queue to be sent to the destination vertex
     * @param mail contains from, to index and message
     */
    public void sendMail(Mail mail){
        this.serverQueue.add(mail);
    }

    /**
     * send message to neighbors with the sendMail function
     * @param edge to embed the id of the destination node to a mail
     * @param message the message to be sent
     */
    public void sendToNeighbor(Edge edge, Message message){
        Mail mail = new Mail(this.id, edge.getToVertexId(), message, MailType.NORMAL);
        sendMail(mail);
    }

    /**
     * send message to neighbors with the sendMail function
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
     * @param mail the mail to be received
     */
    public void onReceive(Mail mail){
        this.status = VertexStatus.ACTIVE;
        incomingQueue.add(mail);
    }

    public void addEdge(Edge edge){
        this.getEdges().add(edge);
    }

    public void addEdge(int to){
        Edge edge = new Edge(this.id,to,null);
        this.addEdge(edge);
    }

    public void addEdge(Vertex<T> to){
        Edge edge = new Edge(this.id,to.getId(),null);
        this.addEdge(edge);
    }

    /**
     * compute incoming messages
     */
    public abstract void compute();

    public enum VertexStatus {
        ACTIVE, INACTIVE
    }
}
