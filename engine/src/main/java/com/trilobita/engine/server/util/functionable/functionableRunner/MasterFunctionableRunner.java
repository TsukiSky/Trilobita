package com.trilobita.engine.server.util.functionable.functionableRunner;

import com.trilobita.core.common.Computable;
import com.trilobita.core.common.Mail;
import com.trilobita.core.common.Mail.MailType;
import com.trilobita.core.common.Message;
import com.trilobita.core.messaging.MessageConsumer;
import com.trilobita.core.messaging.MessageProducer;
import com.trilobita.engine.server.util.functionable.Functionable;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;

/**
 * An FunctionableRunner is a singleton in one worker.
 * It is used to register, initiate, and run functionable instances added by the
 * user.
 */
@Slf4j
public class MasterFunctionableRunner extends FunctionableRunner {
    private static final String MASTER_TOPIC = "MASTER_FUNCTIONAL";
    private static MasterFunctionableRunner instance = null;
    private final MessageConsumer functionableConsumer;
    private final Map<String, CopyOnWriteArrayList<Computable<?>>> functionalValues = new HashMap<>();
    private Boolean isPrimary;

    private MasterFunctionableRunner(Integer serverId, Boolean isPrimary) throws ExecutionException, InterruptedException {
        // put values in functionalValue by insName
        functionableConsumer = new MessageConsumer(MASTER_TOPIC,
                serverId, (key, value, partition, offset) -> {
            if (!this.isPrimary) {
                return;
            }
            if (value != null) {
                if (value.getMailType() == MailType.FUNCTIONAL) {
                    // put values in functionalValue by insName
                    Functionable.FunctionableRepresenter class_value = (Functionable.FunctionableRepresenter) value.getMessage().getContent();
                    this.addToFunctionalValues(class_value.className, class_value.initLastValue);
                }
            }
        });
        if (isPrimary) {
            this.functionableConsumer.start();
        }
        this.isPrimary = isPrimary;
    }

    public synchronized static MasterFunctionableRunner getInstance(Integer id, Boolean isPrimary) throws ExecutionException, InterruptedException {
        if (instance == null) {
            instance = new MasterFunctionableRunner(id, isPrimary);
        }
        return instance;
    }

    private static Functionable<?> initFunctionableInstance(String className, String topic, Computable<?> initLastValue, Computable<?> initNewValue)
            throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException,
            NoSuchMethodException, SecurityException, ClassNotFoundException {

        Class<?> cls = Class.forName(className);
        return topic == null
                ? (Functionable<?>) cls.getConstructor(Computable.class, Computable.class).newInstance(initLastValue, initNewValue)
                : (Functionable<?>) cls.getConstructor(Computable.class, Computable.class, String.class)
                .newInstance(initLastValue, initNewValue, topic);
    }

    public void start() {
        if (this.isPrimary) {
            this.broadcastFunctionables();
        }
    }

    public void stop() throws InterruptedException {
        this.functionableConsumer.stop();
    }

    public void becomePrimary() {
        this.isPrimary = true;
        try {
            this.functionableConsumer.start();
            log.info("functionableConsumer started.");
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * To run all functionable tasks and broadcast to worker
     */
    public void runFunctionableTasks() {
        if (!this.isPrimary) {
            return;
        }
        log.info("Received functional values: {}", this.functionalValues);
        for (Map.Entry<String, CopyOnWriteArrayList<Computable<?>>> entry : this.functionalValues.entrySet()) {
            String instanceName = entry.getKey();
            CopyOnWriteArrayList<Computable<?>> values = entry.getValue();
            if (!values.isEmpty()) {
                Functionable<?> functionable = this.findFunctionableByName(instanceName);
                if (functionable != null) {
                    functionable.execute(values);
                    functionable.sendMail(functionable.getNewFunctionableValue(), true);
                    values.clear(); // reset FunctionableValues
                } else {
                    log.info("No matching functionable found.");
                }
            }
        }
    }

    /**
     * Register and create a functionable instance
     *
     * @param className the class name of the functionable class (not the abstract
     *                  Functionable)
     * @param topicName the topic that the functionable is attached to
     */
    public void registerFunctionable(String className, String topicName, Computable<?> initLastValue, Computable<?> initNewValue) {
        try {
            Functionable<?> functionable = initFunctionableInstance(className, topicName, initLastValue, initNewValue);
            this.registerFunctionable(functionable);
            // create topic if not exist
            if (topicName != null) {
                MessageProducer.createTopic(topicName);
            }
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
                 | NoSuchMethodException | SecurityException | ClassNotFoundException e) {
            log.error("Error registerFunctionable:", e);
        }
    }

    /**
     * Register several functionables by class names and topics
     */
    public void registerFunctionables(Functionable.FunctionableRepresenter[] functionables) {
        for (Functionable.FunctionableRepresenter functionable : functionables) {
            this.registerFunctionable(functionable.className, functionable.topic, functionable.initLastValue, functionable.initNewValue);
        }
    }

    /**
     * Send registered functionable instances to all working servers to their
     * message topics.
     */
    public void broadcastFunctionables() {
        if (this.getFunctionables() != null) {
            for (Functionable<?> functionable : this.getFunctionables()) {
                Mail mail = new Mail(-1, new Message(functionable), Mail.MailType.FUNCTIONAL);
                MessageProducer.produce(null, mail, "INIT_FUNCTIONAL");
            }
            // send stop signal
            Mail mail = new Mail(-1, new Message(this.getFunctionables().size()), Mail.MailType.FINISH_SIGNAL);
            MessageProducer.produce(null, mail, "INIT_FUNCTIONAL");
        }
        log.info("[Functionable] Finished broadcasting Functionables.");
    }

    private void addToFunctionalValues(String insName, Computable<?> value) {
        // If the key is not present in the map, create a new list and put it in the map
        this.functionalValues.putIfAbsent(insName, new CopyOnWriteArrayList<>());
        // Add the value to the list associated with the key
        this.functionalValues.get(insName).add(value);
    }

    public <T> Map<String, Computable<T>> createSnapshot() {
        Map<String, Computable<T>> nameNewvalueMap = new HashMap<>();
        for (Functionable functionable : this.getFunctionables()) {
            nameNewvalueMap.put(functionable.getInstanceName(), functionable.getNewFunctionableValue());
        }
        return nameNewvalueMap;
    }

    public <T> void syncSnapshot(Map<String, Computable<T>> nameNewvalueMap) {
        for (Map.Entry<String, Computable<T>> entry : nameNewvalueMap.entrySet()) {
            Functionable functionable = findFunctionableByName(entry.getKey());
            if (functionable != null) {
                functionable.setNewFunctionableValue(entry.getValue());
            }
        }
    }
}
