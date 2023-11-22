package com.trilobita.engine.server.functionable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.trilobita.commons.Mail;
import com.trilobita.engine.server.Context;

/*
 * Combine outcoming messages from a workerserver to another workerserver.
 */
public abstract class Combiner<T> extends Functionable<T> {
    @Override
    public void execute(Context context) {
        this.combine(context.getOutMailTable());
    }

    // main method for combine
    public void combine(ConcurrentHashMap<Integer, CopyOnWriteArrayList<Mail>> outMailTable) {
        // process hashmap
        outMailTable.forEach((toVertexId, mails) -> {
            List<Mail> mailList = new ArrayList<>();
            Mail newMail = this.combineMails(toVertexId, mails);
            mailList.add(newMail);
            outMailTable.put(toVertexId, new CopyOnWriteArrayList<>(mailList));
        });
    }

    // combine mails
    public abstract Mail combineMails(int toVertexId, CopyOnWriteArrayList<Mail> mails);
}
