package com.trilobita.engine.server.functionable.FunctionableRunner;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.math3.util.Pair;

import com.trilobita.commons.Computable;
import com.trilobita.commons.Mail;
import com.trilobita.commons.Message;
import com.trilobita.commons.Mail.MailType;
import com.trilobita.core.messaging.MessageProducer;
import com.trilobita.engine.server.functionable.Functionable;

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
        clusterIncomingMails(inMailQueue);
        for (Map.Entry<String, List<Computable<?>>> entry : functionalValues.entrySet()) {
            String instanceName = entry.getKey();
            List<Computable<?>> values = entry.getValue();
            Functionable<?> functionable = this.findFunctionableByName(instanceName);
            if (functionable != null) {
                functionable.execute(values);
                functionable.sendMail(functionable.getNewFunctionableValue(), true);
            } else {
                System.out.println("No matching functionable found.");
            }
        }
        this.resetFunctionableValues();

    }

    /**
     * Cluster incoming mails by functinable instance name. Store the values into
     * functionalValues.
     * 
     * @param inMailQueue
     */
    private void clusterIncomingMails(LinkedBlockingQueue<Mail> inMailQueue) {
        LinkedBlockingQueue<Mail> newInMailQueue = new LinkedBlockingQueue<Mail>();
        while (!inMailQueue.isEmpty()) {
            Mail mail = inMailQueue.poll();
            if (mail != null) {
                if (mail.getMailType() == MailType.FUNCTIONAL) {
                    // put values in functinalValue by insName
                    Pair<String, Computable<?>> pair = (Pair<String, Computable<?>>) mail.getMessage().getContent();
                    this.addToFunctionalValues(pair.getKey(), pair.getValue());
                } else {
                    newInMailQueue.add(mail);
                }
            }
        }
        inMailQueue = newInMailQueue;
    }

    /**
     * Register and create a functionable instance
     * 
     * @param className the class name of the functionable class (not the abstract
     *                  Functionable)
     * @param topicName the topic that the functionable is attached to
     */
    public void registerFunctionable(String className, String topicName) {
        try {
            Functionable<?> functionable = initFunctionableInstance(className, topicName);
            this.registerFunctionable(functionable, topicName);
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
                | NoSuchMethodException | SecurityException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Register several functionables by class names and defautlt topics
     * 
     * @param classNames the class names of the functionable classes (not the
     *                   abstract Functionable)
     * @param topicNames the topics that the functionables are attached to
     */
    public void registerFunctionables(String[] classNames, String[] topicNames) {
        assert classNames.length == topicNames.length;
        for (int i = 0; i < classNames.length; i++) {
            this.registerFunctionable(classNames[i], topicNames[i]);
        }
    }

    /**
     * No consumers
     * 
     * @param classNames the class names of the functionable classes (not the
     *                   abstract Functionable)
     */
    public void registerFunctionables(String[] classNames) {
        for (int i = 0; i < classNames.length; i++) {
            this.registerFunctionable(classNames[i], null);
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

    private static Functionable<?> initFunctionableInstance(String className, String topic)
            throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException,
            NoSuchMethodException, SecurityException, ClassNotFoundException {
        Class<?> cls = Class.forName(className);
        Constructor<?> constructor = cls.getConstructor(String.class);
        Functionable<?> functionable = topic == null
                ? (Functionable<?>) constructor.newInstance()
                : (Functionable<?>) constructor.newInstance(topic);
        return functionable;
    }

    private void addToFunctionalValues(String insName, Computable<?> value) {
        // If the key is not present in the map, create a new list and put it in the map
        this.functionalValues.putIfAbsent(insName, new ArrayList<>());
        // Add the value to the list associated with the key
        this.functionalValues.get(insName).add(value);
    }

    private void resetFunctionableValues() {
        for (Map.Entry<String, List<Computable<?>>> entry : functionalValues.entrySet()) {
            List<Computable<?>> values = entry.getValue();
            values.clear();
        }
    }
}
