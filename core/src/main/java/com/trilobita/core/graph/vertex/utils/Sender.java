package com.trilobita.core.graph.vertex.utils;


import com.trilobita.commons.Mail;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class Sender {
    private ConcurrentHashMap<Integer, Mail> messageQueue;
    public Sender(ConcurrentHashMap<Integer, Mail> mail){
        this.messageQueue = mail;
    }

    public void setMails(ConcurrentHashMap<Integer, Mail> mail){
        this.messageQueue = mail;
    }

    public void addToQueue(Mail mail){
        this.messageQueue.putIfAbsent(mail.getToVertexId(), mail);
        this.messageQueue.get(mail.getToVertexId()).add(mail);
    }
}
