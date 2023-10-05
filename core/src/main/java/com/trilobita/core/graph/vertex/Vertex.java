package com.trilobita.core.graph.vertex;

import com.trilobita.commons.Mail;
import com.trilobita.commons.MailType;
import com.trilobita.commons.MessageType;
import com.trilobita.core.graph.vertex.utils.Sender;
import com.trilobita.core.graph.vertex.utils.Value;
import com.trilobita.commons.Message;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

@Data
@AllArgsConstructor
@NoArgsConstructor
public abstract class Vertex {
    private int id;
    private Value state;
    private List<Edge> edges;
    private boolean flag;       // active/idle
    private BlockingQueue<Mail> incomingQueue;
    private Sender sender;


    /**
     * <p>
     *     Send the finish signal to the server
     * </p>
     */
    public void sendFinish(){
//        tell the server that the vertex has finished its job
        Mail mail = new Mail(-1,-1,null, MailType.FINISH_INDICATOR);
        this.getSender().addToQueue(mail);
    }

    /**
     * <p>
     *     Push the mail to the server's queue to be sent
     *     to the destination vertex
     * </p>
     * @param mail contains from, to index and message
     */
    public void sendMail(Mail mail){
        this.getSender().addToQueue(mail);
    }

    /**
     * <p>
     *     send message to neighbors with the sendMail function
     * </p>
     * @param edge to embed the id of the destination node to a mail
     * @param message the message to be sent
     */
    public void sendToNeighbor(Edge edge, Message<?> message){
        Mail mail = new Mail(this.id, edge.getTo().id, new ArrayList<>(), MailType.NORMAL);
        mail.add(message);
        sendMail(mail);
    }

    /**
     * <p>
     *     send message to neighbors with the sendMail function
     * </p>
     * @param to the id of the destination vertex
     * @param message the message to be sent
     */
    public void sendTo(int to, Message<?> message){
        Mail mail = new Mail(this.id, to, new ArrayList<>(), MailType.NORMAL);
        mail.add(message);
        sendMail(mail);
    }

    /**
     * Add the mail to the incoming queue of the vertex
     * @param mail
     */
    public void onReceive(Mail mail){
        incomingQueue.add(mail);
    };


    public void compute(){
        List<Message<?>> processMessages = new ArrayList<>();
        while (!this.getIncomingQueue().isEmpty()){
//            process the message until it reaches the barrier message
            Mail mail = this.getIncomingQueue().poll();
            List<Message<?>> messages = mail.getMessages();
            for (Message<?> message: messages){
                if (message.getMessageType() == MessageType.BARRIER){
                    break;
                }
                processMessages.add(message);
            }
        }
        process(processMessages);
    }

    /**
     * <p>
     *     Compute the updated state
     * </p>
     * @param messages a list of mails used for computing the new state
     */
    public void process(List<Message<?>> messages){
//        update the state according to the incoming messages

    }
}
