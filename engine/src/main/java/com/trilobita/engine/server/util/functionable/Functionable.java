package com.trilobita.engine.server.util.functionable;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.trilobita.core.common.Computable;
import com.trilobita.core.common.Mail;
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

    private static String MASTER_TOPIC = "MASTER_FUNCTIONAL";
    public String instanceName;
    private Computable<T> lastFunctionableValue; // returned by master for last superstep
    private Computable<T> newFunctionableValue; // this superstep
    private String topic = null;
    private MessageConsumer workerMessageConsumer = null;
    private Integer serverId;

    public Functionable(Computable<T> initLastValue, Computable<T> initNewValue) {
        this.instanceName = this.getClass().getName();
        this.lastFunctionableValue = initLastValue;
        this.newFunctionableValue = initNewValue;
    }

    public Functionable(Computable<T> initLastValue, Computable<T> initNewValue, String topic) {
        this.instanceName = this.getClass().getName();
        this.lastFunctionableValue = initLastValue;
        this.newFunctionableValue = initNewValue;
        this.topic = topic;
    }

    public abstract void execute(AbstractServer<?> server);

    public abstract void execute(List<Computable<?>> computables);

    /**
     * Register a consumer to the functionable. Else the default is null.
     *
     * @param workerMessageHandler MessageHandler that handles message on the worker
     */
    public void registerAndStartConsumer(MessageHandler workerMessageHandler) throws ExecutionException, InterruptedException {
        assert this.topic != null;
        this.setTopic(this.topic);
        this.setWorkerMessageConsumer(
                new MessageConsumer(this.topic, this.serverId, workerMessageHandler));
        this.workerMessageConsumer.start();
        log.info("[Functionable] Started {}'s consumer", this.getInstanceName());
    }

    /**
     * Send mail to master/workers if needed.
     * (this.workerMessageConsumer == null) means that no need to communicate
     *
     * @param funcValue      the calculated functionable value tro be sent
     * @param serverIsMaster to decide which topic to sent to
     */
    public void sendMail(Computable<?> funcValue, boolean serverIsMaster) {
        if (this.topic != null) {
            Mail mail = new FunctionalMail(this.instanceName, funcValue);
            String topic = serverIsMaster ? this.topic : MASTER_TOPIC;
            log.info("[Functionable] Send mail {} to {} topic.", funcValue, topic);
            MessageProducer.produce(null, mail, topic);
        }
    }

    public static class FunctionableRepresenter implements Serializable {
        public String className;
        public String topic;
        public Computable<?> initLastValue;
        public Computable<?> initNewValue;

        public FunctionableRepresenter(String className, String topic, Computable<?> initLastValue, Computable<?> initNewValue) {
            this.className = className;
            this.topic = topic;
            this.initLastValue = initLastValue;
            this.initNewValue = initNewValue;
        }

        public FunctionableRepresenter(String className, Computable<?> initLastValue) {
            this.className = className;
            this.initLastValue = initLastValue;
            this.initNewValue = initLastValue;
        }
    }
}
