package com.trilobita.core.graph.vertex;


import com.trilobita.commons.Mail;
import com.trilobita.commons.MailType;
import com.trilobita.commons.Message;
import com.trilobita.commons.MessageType;

import java.util.ArrayList;
import java.util.List;

public class NormalVertex extends AbstractVertex{
    /**
     * <p>
     *     process the mail in its incoming message queue
     *     and compute the updated state
     * </p>
     */
    public void process(){
        List<Mail> mails = new ArrayList<>();
        while (!this.getIncomingQueue().isEmpty()){
//            process the message until it reaches the barrier message
            Mail mail = this.getIncomingQueue().poll();
            Message<?> message = mail.getMessage();
            if (message.getMessageType() == MessageType.BARRIER){
                break;
            }
            mails.add(mail);
        }
        compute(mails);
    }

    /**
     * <p>
     *     Compute the updated state
     * </p>
     * @param mails a list of mails used for computing the new state
     */
    public void compute(List<Mail> mails){
//        update the state according to the incoming messages

    }
}
