package com.trilobita.core.graph.vertex;

import com.trilobita.core.common.Mail;
import com.trilobita.core.common.Computable;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * The vertex in the graph
 * @param <T> the type of the value of the vertex
 */
@Data
public abstract class Vertex<T> implements Serializable {
    private int id;
    private boolean shouldStop;
    private List<Edge> edges;
    private VertexStatus status;
    private Computable<T> value;
    private Computable<T> valueLastSuperstep;
    private boolean stepFinished;
    private BlockingQueue<Mail> incomingQueue;
    private BlockingQueue<Mail> serverQueue;

    public Vertex(int id, Computable<T> value) {
        this.id = id;
        this.value = value;
        this.edges = new ArrayList<>();
        this.status = VertexStatus.INACTIVE;
        this.stepFinished = false;
        this.shouldStop = false;
        this.incomingQueue = new LinkedBlockingQueue<>();
    }

    public void addMailToServerQueue(Mail mail){
        this.serverQueue.add(mail);
    }

    /**
     * Push the mail to the server's queue to be sent to the destination vertex
     */
    public abstract void sendMail();


    public abstract void startSuperstep();

    /**
     * Add the mail to the incoming queue of the vertex
     * @param mail the mail to be received
     */
    public void onReceive(Mail mail){
        this.status = VertexStatus.ACTIVE;
        incomingQueue.add(mail);
    }

    /**
     * Add an edge to the vertex
     * @param edge the edge to be added
     */
    public void addEdge(Edge edge){
        this.getEdges().add(edge);
    }

    /**
     * Add an edge to the vertex
     * @param to the destination vertex id
     */
    public void addEdge(int to){
        Edge edge = new Edge(this.id,to,null);
        this.addEdge(edge);
    }

    /**
     * Add an edge to the vertex
     * @param to the destination vertex
     */
    public void addEdge(Vertex<T> to){
        Edge edge = new Edge(this.id,to.getId(),null);
        this.addEdge(edge);
    }

    public void addEdge(int to, Computable<T> value){
        Edge edge = new Edge(this.id,to,value);
        this.addEdge(edge);
    }


    /**
     * Add an edge to the vertex
     * @param to the destination vertex
     * @param value the edge value
     */
    public void addEdge(Vertex<T> to, Computable<T> value){
        Edge edge = new Edge(this.id,to.getId(),value);
        this.addEdge(edge);
    }



    /**
     * execute the superstep of the vertex
     */
    public void step() {
        this.compute();
    }

    /**
     * compute incoming messages
     */
    public abstract void compute();

    public abstract boolean checkStop(Computable<T> c);

    @Override
    public String toString(){
        return "{" + "vertex id: " + this.id + ", vertex value: " + this.value + ", shouldStop: " + this.shouldStop + "}";
    }

    /**
     * The status of the vertex
     */
    public enum VertexStatus {
        ACTIVE, INACTIVE
    }
}
