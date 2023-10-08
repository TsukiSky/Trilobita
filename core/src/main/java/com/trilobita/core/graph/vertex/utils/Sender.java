package com.trilobita.core.graph.vertex.utils;


import com.trilobita.commons.Mail;

import java.util.concurrent.BlockingQueue;

public class Sender {
    private BlockingQueue<Mail> messageQueue;
    public Sender(BlockingQueue<Mail> messageQueue){
        this.messageQueue = messageQueue;
    }

    public void setMails(BlockingQueue<Mail> messageQueue){
        this.messageQueue = messageQueue;
    }

    public void addToQueue(Mail mail){
        this.messageQueue.add(mail);
    }
}
