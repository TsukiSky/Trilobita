package pagerank.vertex;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.trilobita.commons.*;
import com.trilobita.core.graph.vertex.Edge;
import com.trilobita.core.graph.vertex.Vertex;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;

@Slf4j
@EqualsAndHashCode(callSuper = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
public class PageRankVertex extends Vertex<Double> implements Serializable {
    private final double weight = 0.85;
    private final double epsilon = 0.1;

    public PageRankVertex(int id) {
        super(id, new PageRankValue(0.0));
    }

    @Override
    public void startSuperstep() {
        // initialize the score to be (1-weight) * score in previous superstep
        this.getValue().setValue(1 - weight);
    }

    @Override
    public boolean checkStop(Computable<Double> c) {
        if (this.getValue().getValue() == 0){
            return false;
        }
        return Math.abs(c.getValue()-this.getValue().getValue()) < epsilon;
    }

    @Override
    public void compute() {
        if (!isShouldStop()){
            double oldValue = this.getValue().getValue();
            startSuperstep();
            while (!this.getIncomingQueue().isEmpty()) {
                Message message = this.getIncomingQueue().poll().getMessage();
                PageRankValue score = (PageRankValue) message.getContent();
                this.getValue().add(score.multiply(weight));
            }
//            if (this.checkStop(new PageRankValue(oldValue))){
                this.setShouldStop(true);
//            }
        }
        this.sendMail();
    }

    @Override
    public void sendMail(){
        // finished all the job, generate out mail
        Message msg = new Message(new PageRankValue(this.getValue().getValue()/this.getEdges().size()));
        for (Edge edge : this.getEdges()) {
            int vertexId = edge.getToVertexId();
            Mail mail = new Mail(vertexId, msg, Mail.MailType.NORMAL);
            this.addMailToServerQueue(mail);
        }
    }
}
