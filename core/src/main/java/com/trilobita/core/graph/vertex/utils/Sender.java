package com.trilobita.core.graph.vertex.utils;


import com.trilobita.commons.Mail;

import java.util.concurrent.BlockingQueue;

public class Sender {
    private BlockingQueue<Mail> mails;
    public void addToQueue(Mail mail){
        mails.add(mail);
    }
}
