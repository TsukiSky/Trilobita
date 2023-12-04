package com.trilobita.engine.server.util.functionable.instance.combiner;

import java.util.concurrent.CopyOnWriteArrayList;

import com.trilobita.core.common.Mail;
import com.trilobita.core.common.Computable;
import com.trilobita.core.common.Message;
import com.trilobita.engine.server.util.functionable.Combiner;
import lombok.extern.slf4j.Slf4j;

/*
 * Get the min of all messages sent to the same vertex.
 */
@Slf4j
public class MinCombiner extends Combiner<Double> {

    public MinCombiner(Computable<Double> initLastValue, Computable<Double> initNewValue) {
        super(initLastValue, initNewValue);
    }

    @Override
    public Mail combineMails(Integer toVertexId, CopyOnWriteArrayList<Mail> mails) {

        Mail newMail = new Mail(-1,toVertexId, null, Mail.MailType.NORMAL);
        Double min_value = this.getLastFunctionableValue().getValue();

        Computable<Double> content;
        for (Mail mail : mails) {
            content = (Computable<Double>) mail.getMessage().getContent();

            if (min_value > content.getValue()) {
                min_value = content.getValue();
                newMail.setFromVertexId(mail.getFromVertexId());
            }
        }
        this.getNewFunctionableValue().setValue(min_value);
        newMail.setMessage(new Message(this.getNewFunctionableValue()));
        return newMail;
    }
}
