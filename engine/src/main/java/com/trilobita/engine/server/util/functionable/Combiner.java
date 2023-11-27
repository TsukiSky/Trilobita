package com.trilobita.engine.server.util.functionable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import lombok.extern.slf4j.Slf4j;

import com.trilobita.commons.Computable;
import com.trilobita.commons.Mail;
import com.trilobita.engine.server.AbstractServer;

/*
 * Combine outcoming messages from a workerserver to another workerserver.
 */
@Slf4j
public abstract class Combiner<T> extends Functionable<T> {

    public Combiner(Computable<T> initValue) {
        super(initValue);
        this.functionableType = FunctionableType.COMBINER;
    }

    private Map<Integer, CopyOnWriteArrayList<Mail>> vertexMailMap = new HashMap<Integer, CopyOnWriteArrayList<Mail>>();

    @Override
    public void execute(AbstractServer<?> server) {
        LinkedBlockingQueue<Mail> outMailQueue = server.getOutMailQueue();
        server.outMailQueue.addAll(this.combine(outMailQueue));
        log.info("server.outMailQueue = {}", server.outMailQueue);
    }

    @Override
    public void execute(List<Computable<?>> computables) {
        return;
    }

    // main method for combine
    public LinkedBlockingQueue<Mail> combine(LinkedBlockingQueue<Mail> outMailQueue) {
        LinkedBlockingQueue<Mail> combinedOutMailQueue = new LinkedBlockingQueue<>();
        while (!outMailQueue.isEmpty()) {
            Mail mail = outMailQueue.poll();
            int receiverId = mail.getToVertexId();
            this.addToVertexMailMap(receiverId, mail);
        }
        for (Map.Entry<Integer, CopyOnWriteArrayList<Mail>> map : vertexMailMap.entrySet()) {
            Mail combinedMail = this.combineMails(map.getKey(), map.getValue());
            combinedOutMailQueue.add(combinedMail);
        }
        return combinedOutMailQueue;
    }

    // combine mails
    public abstract Mail combineMails(Integer toVertexId, CopyOnWriteArrayList<Mail> mails);

    private void addToVertexMailMap(Integer receiverVertextId, Mail mail) {
        // If the key is not present in the map, create a new list and put it in the map
        this.vertexMailMap.putIfAbsent(receiverVertextId, new CopyOnWriteArrayList<Mail>());
        // Add the value to the list associated with the key
        this.vertexMailMap.get(receiverVertextId).add(mail);
    }
}
