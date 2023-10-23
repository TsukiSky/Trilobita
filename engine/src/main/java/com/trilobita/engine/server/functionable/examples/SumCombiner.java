package com.trilobita.engine.server.functionable.examples;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.trilobita.commons.*;
import com.trilobita.engine.server.functionable.Combiner;

/*
 * Sum all messages sent to the same vertex.
 */
public class SumCombiner extends Combiner {
    @Override
    public void combine(ConcurrentHashMap<Integer, CopyOnWriteArrayList<Mail>> outMailTable) {
        // process hashmap
        outMailTable.forEach((toVertexId, mails) -> {
            List<Mail> mailList = new ArrayList<>();
            Mail newMail = combineMails(toVertexId, mails);
            mailList.add(newMail);
            outMailTable.put(toVertexId, new CopyOnWriteArrayList<>(mailList));
        });
    }

    // combine mails
    private Mail combineMails(int toVertexId, CopyOnWriteArrayList<Mail> mails) {
        Mail newMail = new Mail(toVertexId, null, MailType.NORMAL);
        Computable newContent = null;
        for (Mail mail: mails) {
            Computable content = (Computable) mail.getMessage().getContent();
            if (newContent == null) {
                newContent = content;
            } else {
                newContent = newContent.add(content);
            }
        }
        newMail.setMessage(new Message(newContent, MessageType.NORMAL));
        return newMail;
    }
}
