package com.trilobita.engine.server.util.functionable.examples.combiners;

import java.util.concurrent.CopyOnWriteArrayList;

import com.trilobita.commons.*;
import com.trilobita.engine.server.util.functionable.Combiner;

/*
 * Sum all messages sent to the same vertex.
 */
public class SumCombiner<T> extends Combiner<T> {
    public SumCombiner(Computable<T> initValue) {
        super(initValue);
    }

    @Override
    public Mail combineMails(Integer toVertexId, CopyOnWriteArrayList<Mail> mails) {
        Mail newMail = new Mail(toVertexId, null, Mail.MailType.NORMAL);
        Computable<T> newContent = null;
        for (Mail mail : mails) {
            Computable<T> content = (Computable) mail.getMessage().getContent();
            if (newContent == null) {
                newContent = content;
            } else {
                newContent = newContent.add(content);
            }
        }
        newMail.setMessage(new Message(newContent));
        return newMail;
    }
}
