package com.trilobita.engine.server.util.functionable.FunctionableRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trilobita.commons.Computable;
import com.trilobita.commons.Mail;
import com.trilobita.commons.Mail.MailType;
import com.trilobita.core.messaging.MessageConsumer;
import com.trilobita.core.messaging.MessageConsumer.MessageHandler;
import com.trilobita.engine.server.AbstractServer;
import com.trilobita.engine.server.util.functionable.Functionable;
import lombok.extern.slf4j.Slf4j;

/**
 * An FunctionableRunner is a singleton in one worker.
 * It is used to register, initiate, and run functionable instances added by the
 * user.
 */
@Slf4j
public class WorkerFunctionableRunner extends FunctionableRunner {

    private static WorkerFunctionableRunner instance = null;
    private MessageConsumer initFunctionablesConsumer;
    private MessageHandler functionalMessageHandler;
    private Map<String, Computable<?>> incomingFunctionableValues = new HashMap<>();
    public boolean finishedRegisterFunctionables = false;

    private WorkerFunctionableRunner(Integer serverId) throws ExecutionException, InterruptedException {
        this.functionalMessageHandler = new MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail value, int partition, long offset)
                    throws JsonProcessingException, InterruptedException, ExecutionException {
                log.info("[Functionable] Received Functionable message {}", value.getMessage().getContent());
                Map<String, Computable<?>> map = (Map) value.getMessage().getContent();
                incomingFunctionableValues.putAll(map);
            }
        };
        this.initFunctionablesConsumer = new MessageConsumer("INIT_FUNCTIONAL",
                serverId, new MessageHandler() {
                    @Override
                    public void handleMessage(UUID key, Mail value, int partition, long offset)
                            throws InterruptedException, ExecutionException {
                        log.info("Received INIT_FUNCTIONAL FUNCTIONAL message from master.");
                        if (value.getMailType() == MailType.FUNCTIONAL) {
                            Functionable<?> functionable = (Functionable<?>) value.getMessage().getContent();
                            functionable.setServerId(serverId);
                            WorkerFunctionableRunner.this.registerFunctionable(functionable);
                            if (functionable.getTopic() != null) {
                                functionable.registerAndStartConsumer(functionalMessageHandler);
                            }
                        } else if (value.getMailType() == MailType.FINISH_SIGNAL) {
                            log.info("Received all functionable instances from master.");
                            // TODO: notify server?
                        }
                    }
                });
        this.initFunctionablesConsumer.start();
        log.info("initFunctionablesConsumer started");
    }

    public synchronized static WorkerFunctionableRunner getInstance(Integer serverId)
            throws ExecutionException, InterruptedException {
        if (instance == null) {
            instance = new WorkerFunctionableRunner(serverId);
        }
        return instance;
    }

    public void finishRegisterFunctionables() {
        try {
            startConsumers();
            this.finishedRegisterFunctionables = false;
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Distribute functional values processed by master from last superstep
     */
    public void distributeValues() {
        for (Map.Entry<String, Computable<?>> map : this.incomingFunctionableValues.entrySet()) {
            String insName = map.getKey();
            Computable lastValue = map.getValue();
            Functionable<?> functionable = this.findFunctionableByName(insName);
            if (functionable != null) {
                functionable.setLastFunctionableValue(lastValue);
            }
        }
        log.info("[Functionable] Finished distributeValues");
    }

    /**
     * To run all functionable tasks in worker and send to master
     */
    public void runFunctionableTasks(AbstractServer<?> server) {
        log.info("runFunctionableTasks started");
        if (this.getFunctionables() != null) {
            for (Functionable<?> functionable : this.getFunctionables()) {
                log.info("Functionable {} is executing...", functionable.instanceName);
                // TODO: add context
                functionable.execute(server);
                log.info("Finished functionable.execute(object);");
                functionable.sendMail(functionable.getNewFunctionableValue(), false);
                log.info("Finished functionable.sendMail;");
            }
            log.info("Finished all functionable tasks;");
        }
    }

    /**
     * Start all consumers for the functionables
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void startConsumers() throws ExecutionException, InterruptedException {
        if (this.getFunctionables() != null) {
            for (Functionable<?> functionable : this.getFunctionables()) {
                MessageConsumer consumer = functionable.getWorkerMessageConsumer();
                if (consumer != null) {
                    consumer.start();
                    log.info("Started {}'s consumer", functionable.getInstanceName());
                }
            }
        }
    }
}
