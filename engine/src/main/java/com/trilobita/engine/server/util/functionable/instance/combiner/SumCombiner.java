package com.trilobita.engine.server.util.functionable.instance.combiner;

import java.util.concurrent.CopyOnWriteArrayList;

import com.trilobita.commons.*;
import com.trilobita.engine.server.util.functionable.Combiner;

/*
 * Sum all messages sent to the same vertex.
 */
public class SumCombiner extends Combiner<Double> {
    public SumCombiner(Computable<Double> initLastValue, Computable<Double> initNewValue) {
        super(initLastValue, initNewValue);
    }

    @Override
    public Mail combineMails(Integer toVertexId, CopyOnWriteArrayList<Mail> mails) {
        Mail newMail = new Mail(toVertexId, null, Mail.MailType.NORMAL);
        Double sum = this.getLastFunctionableValue().getValue();

        Computable<Double> content;
        for (Mail mail : mails) {
            content = (Computable<Double>) mail.getMessage().getContent();
            sum += content.getValue();
        }
        this.getNewFunctionableValue().setValue(sum);
        newMail.setMessage(new Message(this.getNewFunctionableValue()));
        return newMail;
    }
}