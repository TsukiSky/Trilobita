package shortestpath.vertex;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.trilobita.commons.Mail;
import com.trilobita.commons.Message;
import com.trilobita.core.graph.vertex.Edge;
import com.trilobita.core.graph.vertex.Vertex;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
@EqualsAndHashCode(callSuper = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public class ShortestPathVertex extends Vertex<Double> implements Serializable {

    public ShortestPathVertex(int id) {
        super(id, new ShortestPathValue(Double.MAX_VALUE));
    }

    public ShortestPathVertex(int id,Double value) {
        super(id, new ShortestPathValue(value));
    }

    @Override
    public void startSuperstep() {
        this.getValue().setValue((double) 0);
        this.sendMail();
    }

    @Override
    public void compute() {
//        startSuperstep();
        List<Double> allvalue = new ArrayList<>();
        while (!this.getIncomingQueue().isEmpty()) {
            Message message = this.getIncomingQueue().poll().getMessage();
            ShortestPathValue score = (ShortestPathValue) message.getContent();
            allvalue.add(score.getValue());
        }
        Double minvalue = Double.MAX_VALUE;
        if (!allvalue.isEmpty()) {
            minvalue = Collections.min(allvalue);
            log.info("the min value received{}",minvalue);
        } else {
            log.info("the list is empty");
        }
        if (minvalue<this.getValue().getValue()){
            log.info("We are here the min value received is{} and current value is {}",minvalue, this.getValue().getValue());
            this.getValue().setValue(minvalue);
            this.sendMail();
        }
    }

    @Override
    public void sendMail(){
        // finished all the job, generate out mail
        for (Edge edge : this.getEdges()) {
            log.info("the vertex value {} and edge value {}",this.getValue().getValue(),(double)edge.getState().getValue());
            ShortestPathValue shortestPathValue = new ShortestPathValue(this.getValue().getValue() +(double)edge.getState().getValue());
            Message msg = new Message(shortestPathValue);
            int vertexId = edge.getToVertexId();
            Mail mail = new Mail(vertexId, msg, Mail.MailType.NORMAL);
            this.addMailToServerQueue(mail);
        }
    }
}
