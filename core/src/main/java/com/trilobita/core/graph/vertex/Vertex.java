package com.trilobita.core.graph.vertex;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.trilobita.commons.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
public abstract class Vertex<T> implements Serializable {
    private int id;
    private List<Edge> edges;
    private VertexStatus status;
    private Computable<T> value;
    private boolean stepFinished;
    @JsonDeserialize(as = LinkedBlockingQueue.class)
    private BlockingQueue<Mail> incomingQueue;
    @JsonDeserialize(as = LinkedBlockingQueue.class)
    private BlockingQueue<Mail> serverQueue;
    @JsonDeserialize(as = ConcurrentHashMap.class)
    private ConcurrentHashMap<Integer, Computable<T>> vertexValues;

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
        Mail mail = new Mail(this.id, edge.getToVertexId(), message, Mail.MailType.NORMAL);
        sendMail(mail);
    }

    /**
     * send message to neighbors with the sendMail function
     * @param to the id of the destination vertex
     * @param message the message to be sent
     */
    public void sendTo(int to, Message message){
        Mail mail = new Mail(this.id, to, message, Mail.MailType.NORMAL);
        sendMail(mail);
    }

    public void startSuperstep(){

    }

    public void updateServerTempValue(){
        this.vertexValues.put(this.id, this.value);
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
