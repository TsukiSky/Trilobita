package com.trilobita.engine.server.functionable.examples;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.trilobita.commons.Computable;
import com.trilobita.commons.Mail;
import com.trilobita.engine.server.functionable.Combiner;
import com.trilobita.commons.Message;
import com.trilobita.core.graph.vertex.utils.Sender;

/*
 * Sum all messages sent to the same vertex.
 */
public class SumCombiner<T> extends Combiner<T> {

    @Override
    public void combine(ConcurrentHashMap<Integer, CopyOnWriteArrayList<Mail>> outMailTable, Sender sender) {

        // process hashmap
        for (Map.Entry<Integer, CopyOnWriteArrayList<Mail>> entry : outMailTable.entrySet()) {

            CopyOnWriteArrayList<Mail> mails = entry.getValue();

            // combine all mailes
            CopyOnWriteArrayList<Mail> newMails = combineMails(mails);

            // send combined messages to outMailQueue
            sendToOutMailQueue(sender, newMails);

        }

    }

    // combine mails sent to the same worker
    private CopyOnWriteArrayList<Mail> combineMails(CopyOnWriteArrayList<Mail> mails) {

        Computable<T> newMails = null;

        for (Mail mail : mails) {
            List<Message<?>> messages = mail.getMessages();
            // sum all messages
            for (Message<?> message : messages) {
                Computable<T> content = (Computable<T>) message.getContent();
                if (newMails == null) {
                    newMails = content;
                } else {
                    newMails.combine(content);
                }

            }

        }

        return mails;
    }

    // send to outMailQueue
    private void sendToOutMailQueue(Sender sender, CopyOnWriteArrayList<Mail> newMails) {
        // TODO: how to send?
        for (Mail mail : newMails) {
            sender.addToQueue(mail);
        }
    }
}
