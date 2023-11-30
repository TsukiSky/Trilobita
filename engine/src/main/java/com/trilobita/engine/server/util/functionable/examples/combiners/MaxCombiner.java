package com.trilobita.engine.server.util.functionable.examples.combiners;

import java.util.concurrent.CopyOnWriteArrayList;

import com.trilobita.commons.*;
import com.trilobita.engine.server.util.functionable.Combiner;

/*
 * Get the max of all messages sent to the same vertex.
 */
public class MaxCombiner extends Combiner<java.lang.Double> {

    public MaxCombiner(Computable<Double> initLastValue, Computable<Double> initNewValue) {
        super(initLastValue, initNewValue);
    }
    @Override
    public Mail combineMails(Integer toVertexId, CopyOnWriteArrayList<Mail> mails) {
        Mail newMail = new Mail(toVertexId, null, Mail.MailType.NORMAL);
        Double min_value = this.getLastFunctionableValue().getValue();

        Computable<Double> content;
        for (Mail mail : mails) {
            content = (Computable<Double>) mail.getMessage().getContent();

            if (min_value < content.getValue()) {
                min_value = content.getValue();
            }
        }
        this.getNewFunctionableValue().setValue(min_value);
        newMail.setMessage(new Message(this.getNewFunctionableValue()));
        return newMail;
    }
}
