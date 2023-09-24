package com.trilobita.core.graph.vertex;

import com.trilobita.commons.Mail;
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
    public void send(Mail mail){

    };
    public void onReceive(Mail mail){
        incomingQueue.add(mail);
    };

}
