package com.trilobita.engine.server.util.functionable.FunctionableRunner;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

import com.trilobita.commons.Computable;
import com.trilobita.commons.Mail;
import com.trilobita.commons.Message;
import com.trilobita.commons.Mail.MailType;
import com.trilobita.core.messaging.MessageProducer;
import com.trilobita.engine.server.util.functionable.Functionable;
import com.trilobita.engine.server.util.functionable.examples.ExampleFunctionable;

import lombok.extern.slf4j.Slf4j;

/**
 * An FunctionableRunner is a singleton in one worker.
 * It is used to register, initiate, and run functionable instances added by the
 * user.
 */
@Slf4j
public class MasterFunctionableRunner extends FunctionableRunner {
    private static MasterFunctionableRunner instance = null;
    private Map<String, List<Computable<?>>> functionalValues = new HashMap<>();

    public synchronized static MasterFunctionableRunner getInstance() {
        if (instance == null) {
            instance = new MasterFunctionableRunner();
        }
        return instance;
    }

    /**
     * To run all functionable tasks and broadcast to worker
     */
    public void runFunctionableTasks(LinkedBlockingQueue<Mail> inMailQueue) {
        log.info("Started runFunctionableTasks...");
        clusterIncomingMails(inMailQueue);
        log.info("Received functionalValues: {}", functionalValues);
        for (Map.Entry<String, List<Computable<?>>> entry : functionalValues.entrySet()) {
            String instanceName = entry.getKey();
            List<Computable<?>> values = entry.getValue();
            Functionable<?> functionable = this.findFunctionableByName(instanceName);
            if (functionable != null) {
                functionable.execute(values);
                log.info("Executed {}",values);
                functionable.sendMail(functionable.getNewFunctionableValue(), true);
                log.info("Sent Mails.");
                values.clear(); // resetFunctionableValues
            } else {
               log.info("No matching functionable found.");
            }
        }
    }

    /**
     * Cluster incoming mails by functinable instance name. Store the values into
     * functionalValues.
     *
     * @param inMailQueue inMailQueue of the server that store functionable msgs form workers
     */
    private void clusterIncomingMails(LinkedBlockingQueue<Mail> inMailQueue) {
        LinkedBlockingQueue<Mail> newInMailQueue = new LinkedBlockingQueue<Mail>();
        while (!inMailQueue.isEmpty()) {
            Mail mail = inMailQueue.poll();
            if (mail != null) {
                if (mail.getMailType() == MailType.FUNCTIONAL) {
                    // put values in functinalValue by insName
                    ExampleFunctionable class_value = (ExampleFunctionable) mail.getMessage().getContent();
                    this.addToFunctionalValues(class_value.className, class_value.value);
                } else {
                    newInMailQueue.add(mail);
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
    public void registerFunctionable(String className, String topicName, Computable<?> initValue) {
        try {
            Functionable<?> functionable = initFunctionableInstance(className, topicName, initValue);
            this.registerFunctionable(functionable);
            // create topic if not exist
            if (topicName != null){
                MessageProducer.createAndProduce(null, new Mail(new Message(functionable),MailType.START_SIGNAL), topicName);
            }
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
                | NoSuchMethodException | SecurityException | ClassNotFoundException e) {
            log.error("Error registerFunctionable:",e);
        }
    }

    /**
     * Register several functionables by class names and defautlt topics
     */
    public void registerFunctionables(ExampleFunctionable[] functionables) {
        for (ExampleFunctionable functionable : functionables) {
            this.registerFunctionable(functionable.className, functionable.topic, functionable.value);
        }
    }

    // send functionable instances to all workers
    public void broadcastFunctionables() {
        log.info("master broadcasting Functionables");
        if (this.getFunctionables() != null) {
            for (Functionable<?> functionable : this.getFunctionables()) {
                Mail mail = new Mail(-1, new Message(functionable), Mail.MailType.FUNCTIONAL);
                MessageProducer.createAndProduce(null, mail, "INIT_FUNCTIONAL");
            }
            // send stop signal
            Mail mail = new Mail(-1, new Message(this.getFunctionables().size()), Mail.MailType.FINISH_SIGNAL);
            MessageProducer.createAndProduce(null, mail, "INIT_FUNCTIONAL");
        }

    }

    private static Functionable<?> initFunctionableInstance(String className, String topic, Computable<?> initValue)
            throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException,
            NoSuchMethodException, SecurityException, ClassNotFoundException {

        Class<?> cls = Class.forName(className);
        return topic == null
                ? (Functionable<?>) cls.getConstructor(Computable.class).newInstance(initValue)
                : (Functionable<?>) cls.getConstructor(Computable.class, String.class)
                        .newInstance(initValue, topic);
    }

    private void addToFunctionalValues(String insName, Computable<?> value) {
        // If the key is not present in the map, create a new list and put it in the map
        this.functionalValues.putIfAbsent(insName, new ArrayList<>());
        // Add the value to the list associated with the key
        this.functionalValues.get(insName).add(value);
    }
}