package com.trilobita.core.graph.vertex;


import com.trilobita.commons.Mail;
import com.trilobita.commons.MailType;

import java.util.ArrayList;
import java.util.List;

public class NormalVertex extends AbstractVertex{
    public void process(){
        List<Mail> mails = new ArrayList<>();
        while (!this.getIncomingQueue().isEmpty()){
//            process the message until it reaches the barrier message
            Mail mail = this.getIncomingQueue().poll();
            MailType mailType = mail.getMailType();
            if (mailType == MailType.BARRIER){
                break;
            }
            mails.add(mail);
        }
        compute();
    }
    public void compute(){
//        update the state according to the incoming messages

    }
}
