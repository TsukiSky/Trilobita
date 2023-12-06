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

        Computable<Double> min_value = this.getLastFunctionableValue().clone();
        int fromVertexId = -1;
        for (Mail mail : mails) {
            Computable<Double> content = (Computable<Double>) mail.getMessage().getContent();

            if (min_value.getValue() > content.getValue()) {
                min_value.setValue(content.getValue());
                fromVertexId = mail.getFromVertexId();
            }
        }
        return new Mail(fromVertexId, toVertexId, new Message(min_value), Mail.MailType.NORMAL);
    }
}
