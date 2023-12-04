package com.trilobita.engine.server.util.functionable.instance.combiner;

import java.util.concurrent.CopyOnWriteArrayList;

import com.trilobita.core.common.Mail;
import com.trilobita.core.common.Computable;
import com.trilobita.core.common.Message;
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
            content.setValue(sum);
            newMail.setFromVertexId(mail.getFromVertexId());
            newMail.setMessage(new Message(content));
        }
        return newMail;
    }
}
