package com.trilobita.engine.server.functionable.examples.combiners;

import java.util.concurrent.CopyOnWriteArrayList;

import com.trilobita.commons.*;
import com.trilobita.engine.server.functionable.Combiner;

/*
 * Sum all messages sent to the same vertex.
 */
public class MaxCombiner<T> extends Combiner<T> {

    @Override
    public Mail combineMails(Integer toVertexId, CopyOnWriteArrayList<Mail> mails) {
        Mail newMail = new Mail(toVertexId, null, Mail.MailType.NORMAL);
        Computable newContent = null;
        for (Mail mail : mails) {
            Computable<T> content = (Computable) mail.getMessage().getContent();
            if (newContent == null) {
                newContent = content;
            } else {
                if (newContent.compareTo(content) < 0) {
                    newContent = content;
                }
            }
        }
        newMail.setMessage(new Message(newContent));
        return newMail;
    }
}
