package com.trilobita.engine.server.util.functionable.instance.combiner;

import java.util.concurrent.CopyOnWriteArrayList;

import com.trilobita.core.common.Mail;
import com.trilobita.core.common.Computable;
import com.trilobita.core.common.Message;
import com.trilobita.engine.server.util.functionable.Combiner;
import lombok.extern.slf4j.Slf4j;

/*
 * Sum all messages sent to the same vertex.
 */
@Slf4j
public class SumCombiner extends Combiner<Double> {
    public SumCombiner(Computable<Double> initLastValue, Computable<Double> initNewValue) {
        super(initLastValue, initNewValue);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Mail combineMails(Integer toVertexId, CopyOnWriteArrayList<Mail> mails) {
        Computable<Double> sum = this.getLastFunctionableValue().clone();
        for (Mail mail : mails) {
            Computable<Double> content = (Computable<Double>) mail.getMessage().getContent();
            sum.add(content);
        }
        return new Mail(-1, toVertexId, new Message(sum), Mail.MailType.NORMAL);
    }
}
