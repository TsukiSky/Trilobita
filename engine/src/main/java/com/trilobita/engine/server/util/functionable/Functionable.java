package com.trilobita.engine.server.util.functionable;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.trilobita.commons.Computable;
import com.trilobita.commons.Mail;
import com.trilobita.core.messaging.MessageConsumer;
import com.trilobita.core.messaging.MessageProducer;
import com.trilobita.core.messaging.MessageConsumer.MessageHandler;
import com.trilobita.engine.server.AbstractServer;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/*
 * An abstract class for easy adding or removing functional blocks, 
 * We provide the implementation of Combiner and Aggregator, as discussed in Pregel.
 */
@Data
@Slf4j
public abstract class Functionable<T> implements Serializable {

    public String instanceName;
    public FunctionableType functionableType;
    private Computable<T> lastFunctionableValue; // returned by master for last superstep
    private Computable<T> newFunctionableValue; // this superstep
    private String topic = null;
    private static String MASTER_TOPIC = "SERVER_0_MESSAGE";
    private MessageConsumer workerMessageConsumer = null;
    private Integer serverId;

    public abstract void execute(AbstractServer<?> server);

    public abstract void execute(List<Computable<?>> computables);

    public Functionable(Computable<T> initValue) {
        this.instanceName = this.getClass().getName();
        this.lastFunctionableValue = initValue;
        this.newFunctionableValue = initValue;
    }

    public Functionable(Computable<T> initValue, String topic) {
        this.instanceName = this.getClass().getName();
        this.lastFunctionableValue = initValue;
        this.newFunctionableValue = initValue;
        this.topic = topic;
    }

    /**
     * Register a consumer to the functionable. Else the default is null.
     * 
     * @param topic                topic that the consumer consumes
     * @param workerMessageHandler MessageHandler that handles message on the worker
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void registerAndStartConsumer(MessageHandler workerMessageHandler) throws ExecutionException, InterruptedException {
        assert this.topic != null;
        this.setTopic(this.topic);
        this.setWorkerMessageConsumer(
                new MessageConsumer(this.topic, this.serverId, workerMessageHandler));
        this.workerMessageConsumer.start();
        log.info("Started {}'s consumer", this.getInstanceName());
    }

    /**
     * Send mail to master/workers if needed.
     * (this.workerMessageConsumer == null) means that no need to communicate
     * 
     * @param mail
     */
    public void sendMail(Computable<?> computable, boolean serverIsMaster) {
        if (this.workerMessageConsumer != null) {
            Mail mail = new FunctionalMail(this.instanceName, computable);
            String topic = serverIsMaster ? this.topic : MASTER_TOPIC;
            MessageProducer.createAndProduce(null, mail, topic);
        }
    }

    public enum FunctionableType{
        AGGREGATOR,
        COMBINER,
    }
}
