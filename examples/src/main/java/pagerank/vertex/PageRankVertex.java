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
    private final double epsilon = 0.001;

    public PageRankVertex(int id) {
        super(id, new PageRankValue(0.0));
    }

    @Override
    public void startSuperstep() {
        // initialize the score to be (1-weight) * score in previous superstep
        this.getValue().setValue(1 - weight);
    }

    @Override
    public void compute() {
        startSuperstep();
        while (!this.getIncomingQueue().isEmpty()) {
            Message message = this.getIncomingQueue().poll().getMessage();
            PageRankValue score = (PageRankValue) message.getContent();
            // update the state of the vertex according to the incoming score
            this.getValue().add(score.multiply(weight));
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
