package com.trilobita.engine.server.functionable.examples;

import java.util.concurrent.CopyOnWriteArrayList;

import com.trilobita.commons.Mail;
import com.trilobita.engine.server.functionable.FunctionalMailsHandler;

public class AggregatorMailsHandler implements FunctionalMailsHandler{

    @Override
    public void handleMails( CopyOnWriteArrayList<Mail> mails) {
        for (Mail mail : mails) {
            if (mail.getMailType() == Mail.MailType.BROADCAST) {
                // TODO broadcast mails
                
            }
        }
        
    }
    
}
