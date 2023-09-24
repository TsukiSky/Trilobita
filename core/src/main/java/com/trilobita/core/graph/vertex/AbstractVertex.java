package com.trilobita.core.graph.vertex;

import com.trilobita.commons.Mail;
import com.trilobita.commons.MailType;
import com.trilobita.core.graph.vertex.utils.Sender;
import com.trilobita.core.graph.vertex.utils.Value;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.concurrent.BlockingQueue;

@Data
@AllArgsConstructor
@NoArgsConstructor
public abstract class AbstractVertex {
    private int id;
    private Value state;
    private List<Edge> edges;
    private BlockingQueue<Mail> incomingQueue;
    private boolean stepFinish;
    private Sender sender;


    public void sendFinish(){
//        tell the server that the vertex has finished its job
        Mail mail = new Mail(id,-1,null, MailType.FINISH_INDICATOR);
        this.getSender().addToQueue(mail);
    }

    public void send(Mail mail){

    };
    public void onReceive(Mail mail){
        incomingQueue.add(mail);
    };

}
