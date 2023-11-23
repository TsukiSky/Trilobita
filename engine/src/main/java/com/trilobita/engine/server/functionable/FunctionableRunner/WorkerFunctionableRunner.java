package com.trilobita.engine.server.functionable.FunctionableRunner;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trilobita.commons.Computable;
import com.trilobita.commons.Mail;
import com.trilobita.commons.Mail.MailType;
import com.trilobita.core.messaging.MessageConsumer;
import com.trilobita.core.messaging.MessageConsumer.MessageHandler;
import com.trilobita.engine.server.Context;
import com.trilobita.engine.server.functionable.Functionable;

/**
 * An FunctionableRunner is a singleton in one worker.
 * It is used to register, initiate, and run functionable instances added by the
 * user.
 */
public class WorkerFunctionableRunner extends FunctionableRunner {

    private static WorkerFunctionableRunner instance = null;
    private boolean finishedRegisterFunctionables = false;
    private static int POSITIVE_INF = 10000;
    private MessageConsumer initFunctionablesConsumer;
    private Context serverContext;
    private MessageHandler functionalMessageHandler;
    private Map<String, Computable<?>> incomingFunctionableValues;

    private WorkerFunctionableRunner(Context serverContext) {
        instance = new WorkerFunctionableRunner(serverContext);
        this.serverContext = serverContext;
        this.functionalMessageHandler = new MessageHandler() {
            @Override
            public void handleMessage(UUID key, Mail value, int partition, long offset)
                    throws JsonProcessingException, InterruptedException, ExecutionException {
                Map<String, Computable<?>> map = (Map) value.getMessage().getContent();
                incomingFunctionableValues.putAll(map);
            }
        };
        this.initFunctionablesConsumer = new MessageConsumer("INIT_FUNCTIONAL",
                this.serverContext.getServerId(), new MessageHandler() {
                    @Override
                    public void handleMessage(UUID key, Mail value, int partition, long offset)
                            throws InterruptedException {
                        int totalNumFunc = POSITIVE_INF; // a very large number
                        if (value.getMailType() == MailType.FUNCTIONAL) {
                            Functionable<?> functionable = (Functionable<?>) value.getMessage().getContent();
                            functionable.setServerId(serverContext.getServerId());
                            WorkerFunctionableRunner.this.registerFunctionable(functionable);
                            if (functionable.getTopic() != null) {
                                functionable.registerConsumer(functionalMessageHandler);
                            }
                            if (totalNumFunc < POSITIVE_INF) {
                                if (WorkerFunctionableRunner.this.getFunctionables().size() == totalNumFunc) {
                                    WorkerFunctionableRunner.this.finishedRegisterFunctionables = true;
                                }
                            }
                        } else if (value.getMailType() == MailType.FINISH_SIGNAL) {
                            totalNumFunc = (int) value.getMessage().getContent();
                            if (WorkerFunctionableRunner.this.getFunctionables().size() == totalNumFunc) {
                                WorkerFunctionableRunner.this.finishedRegisterFunctionables = true;
                            }
                        }
                    }
                });
    }

    public synchronized static WorkerFunctionableRunner getInstance(Context serverContext) {
        if (instance == null) {
            instance = new WorkerFunctionableRunner(serverContext);
        }
        return instance;
    }

    public void finishRegisterFunctionables() {
        if (finishedRegisterFunctionables) {
            try {
                this.initFunctionablesConsumer.stop();
                startConsumers();
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }

        }

    }

    /**
     * Distribute functional values process by master from last superstep
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
    }

    /**
     * To run all functionable tasks in worker and send to master
     */
    public void runFunctionableTasks() {
        for (Functionable<?> functionable : this.getFunctionables()) {
            functionable.execute(this.serverContext);
            functionable.sendMail(functionable.getNewFunctionableValue(), false);
        }
    }

    /**
     * Start all consumers for the functionables
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void startConsumers() throws ExecutionException, InterruptedException {
        for (Functionable<?> functionable : this.getFunctionables()) {
            MessageConsumer consumer = functionable.getWorkerMessageConsumer();
            if (consumer != null) {
                consumer.start();
            }
        }
    }

    public void start() {
        try {
            this.initFunctionablesConsumer.start();
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
