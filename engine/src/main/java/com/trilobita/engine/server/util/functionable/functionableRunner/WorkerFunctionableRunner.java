package com.trilobita.engine.server.util.functionable.functionableRunner;

import java.util.*;
import java.util.concurrent.ExecutionException;

import com.trilobita.core.common.Computable;
import com.trilobita.core.common.Mail;
import com.trilobita.core.common.Mail.MailType;
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
    private List<Functionable.FunctionableRepresenter> incomingFunctionableValues = new ArrayList<>();

    private WorkerFunctionableRunner(Integer serverId) throws ExecutionException, InterruptedException {
        this.initFunctionablesConsumer = new MessageConsumer("INIT_FUNCTIONAL",
                serverId, new MessageHandler() {
                    @Override
                    public void handleMessage(UUID key, Mail value, int partition, long offset)
                            throws InterruptedException, ExecutionException {
//                        log.info("[Functionable] Received INIT_FUNCTIONAL message from master.");
                        if (value.getMailType() == MailType.FUNCTIONAL) {
                            Functionable<?> functionable = (Functionable<?>) value.getMessage().getContent();
                            functionable.setServerId(serverId);
                            WorkerFunctionableRunner.this.registerFunctionable(functionable);
                            if (functionable.getTopic() != null) {
                                functionable.registerAndStartConsumer(new MessageHandler() {
                                    @Override
                                    public void handleMessage(UUID key, Mail value, int partition, long offset) throws InterruptedException, ExecutionException {
                                        log.info("[Functionable] Received Functionable message {}", value.getMessage().getContent());
                                        Functionable.FunctionableRepresenter functionableRepresenter = (Functionable.FunctionableRepresenter) value.getMessage().getContent();
                                        incomingFunctionableValues.add(functionableRepresenter);
                                    }
                                });
                            }
                        } else if (value.getMailType() == MailType.FINISH_SIGNAL) {
                            log.info("Received all functionable instances from master.");
                            // TODO: notify server?
                        }
                    }
                });
        this.initFunctionablesConsumer.start();
    }

    public synchronized static WorkerFunctionableRunner getInstance(Integer serverId)
            throws ExecutionException, InterruptedException {
        if (instance == null) {
            instance = new WorkerFunctionableRunner(serverId);
        }
        return instance;
    }

    public void stop() throws InterruptedException {
        this.initFunctionablesConsumer.stop();
    }

    /**
     * Distribute functional values processed by master from last superstep
     */
    public void distributeValues() {
        for (Functionable.FunctionableRepresenter functionableSet: this.incomingFunctionableValues) {
            String insName = functionableSet.className;
            Computable<?> lastValue = functionableSet.initLastValue;
            Functionable functionable = this.findFunctionableByName(insName);
            if (functionable != null) {
                functionable.setLastFunctionableValue(lastValue);
            }
        }
//        log.info("[Functionable] Finished distributeValues");
    }

    /**
     * To run all functionable tasks in worker and send to master
     */
    public void runFunctionableTasks(AbstractServer<?> server) {
        if (this.getFunctionables() != null) {
            for (Functionable<?> functionable : this.getFunctionables()) {
                log.info("Functionable {} is executing...", functionable.instanceName);
                // TODO: add context
                functionable.execute(server);
                functionable.sendMail(functionable.getNewFunctionableValue(), false);
            }
            log.info("Finished all functionable tasks;");
        }
    }
}
