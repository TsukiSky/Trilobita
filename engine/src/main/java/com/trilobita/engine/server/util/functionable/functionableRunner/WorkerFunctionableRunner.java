package com.trilobita.engine.server.util.functionable.functionableRunner;

import com.trilobita.commons.Computable;
import com.trilobita.commons.Mail.MailType;
import com.trilobita.core.messaging.MessageConsumer;
import com.trilobita.engine.server.AbstractServer;
import com.trilobita.engine.server.util.functionable.Functionable;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

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
                serverId, (key, value, partition, offset) -> {
                    if (value.getMailType() == MailType.FUNCTIONAL) {
                        Functionable<?> functionable = (Functionable<?>) value.getMessage().getContent();
                        functionable.setServerId(serverId);
                        WorkerFunctionableRunner.this.registerFunctionable(functionable);
                        if (functionable.getTopic() != null) {
                            functionable.registerAndStartConsumer((key1, value1, partition1, offset1) -> {
                                Functionable.FunctionableRepresenter functionableRepresenter = (Functionable.FunctionableRepresenter) value1.getMessage().getContent();
                                incomingFunctionableValues.add(functionableRepresenter);
                            });
                        }
                    } else if (value.getMailType() == MailType.FINISH_SIGNAL) {
                        log.info("[Functionable] Received all functionable instances from master.");
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
                log.info("[Functionable] Master sends back to {}: {}",functionable.getInstanceName(),lastValue);
            }
        }
        this.incomingFunctionableValues.clear();
    }

    /**
     * To run all functionable tasks in worker and send to master
     */
    public void runFunctionableTasks(AbstractServer<?> server) {
        if (this.getFunctionables() != null) {
            for (Functionable<?> functionable : this.getFunctionables()) {
                log.info("Functionable {} is executing...", functionable.instanceName);
                functionable.execute(server);
                functionable.sendMail(functionable.getNewFunctionableValue(), false);
            }
            log.info("Finished all functionable tasks;");
        }
    }
}
