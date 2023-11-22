package com.trilobita.engine.server.functionable;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trilobita.commons.Computable;
import com.trilobita.commons.Mail;
import com.trilobita.core.messaging.MessageConsumer;
import com.trilobita.core.messaging.MessageProducer;
import com.trilobita.core.messaging.MessageConsumer.MessageHandler;
import com.trilobita.engine.server.Context;

import lombok.Data;

/*
 * An abstract class for easy adding or removing functional blocks, 
 * We provide the implementation of Combiner and Aggregator, as discussed in Pregel.
 */
@Data
public abstract class Functionable<T> {

    private Computable<T> functionableValue;
    private String topic = null;
    private MessageConsumer workerMessageConsumer = null;
    private MessageHandler masterMessageHandler = null;
    private MessageHandler workerMessageHandler = null;
    public static Integer MASTER_ID = 0; // TODO: TO BE DELETED

    public abstract void execute(Context context);

    // if has consumer
    public Functionable(String topic, MessageHandler workerMessageHandler,
            MessageHandler masterMessageHandler) {

        registerConsumer(topic, workerMessageHandler, masterMessageHandler);
    }

    public Functionable() {

    }

    /**
     * Register a consumer to the functionable. Else the default is null.
     * 
     * @param topic                topic that the consumer consumes
     * @param workerMessageHandler MessageHandler that handles message on the worker
     * @param masterMessageHandler MessageHandler that handles message on the master
     */
    public void registerConsumer(String topic, MessageHandler workerMessageHandler,
            MessageHandler masterMessageHandler) {
        this.setTopic(topic);
        this.setWorkerMessageHandler(workerMessageHandler);
        this.setMasterMessageHandler(masterMessageHandler);
        this.setWorkerMessageConsumer(
                new MessageConsumer(topic, MASTER_ID, this.getWorkerMessageHandler()));
    }

    /**
     * Send mail to the topic
     * 
     * @param mail
     */
    public void sendMail(Mail mail) {
        MessageProducer.createAndProduce(null, mail, this.getTopic());
    }
}
