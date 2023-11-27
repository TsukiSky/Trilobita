package com.trilobita.engine.server.util.functionable.examples.combiners;

import java.util.concurrent.CopyOnWriteArrayList;

import com.trilobita.commons.*;
import com.trilobita.engine.server.util.functionable.Combiner;

/*
 * Sum all messages sent to the same vertex.
 */
public class MaxCombiner<T> extends Combiner<T> {

    public MaxCombiner(Computable<T> initValue) {
        super(initValue);
    }

    @Override
    public Mail combineMails(Integer toVertexId, CopyOnWriteArrayList<Mail> mails) {
        Mail newMail = new Mail(toVertexId, null, Mail.MailType.NORMAL);
        
        for (Mail mail : mails) {
            Computable<T> content = (Computable<T>) mail.getMessage().getContent();
            if (this.getNewFunctionableValue().compareTo(content.getValue()) < 0) {
                this.getNewFunctionableValue().setValue(content.getValue());
            }
        }
        newMail.setMessage(new Message(this.getNewFunctionableValue()));

        // reset
        this.getNewFunctionableValue().setValue(this.getLastFunctionableValue().getValue());
        return newMail;
    }
}
