package com.trilobita.engine.computing.task;

import com.trilobita.commons.Mail;
import com.trilobita.core.messaging.MessageProducer;
import lombok.Getter;

@Getter
public class MailingTask extends Task {
    private final Mail mail;
    private final int receiverId;

    public MailingTask(Mail mail, int receiverId) {
        this.mail = mail;
        this.receiverId = receiverId;
    }

    @Override
    public void run() {
        MessageProducer.createAndProduce(null, mail, receiverId+"");
    }
}
