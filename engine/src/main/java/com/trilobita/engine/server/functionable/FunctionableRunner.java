package com.trilobita.engine.server.functionable;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import com.trilobita.core.messaging.MessageConsumer;
import com.trilobita.engine.server.Context;

/**
 * An FunctionableRunner is a singleton in one worker.
 * It is used to register, initiate, and run functionable instances added by the
 * user.
 */
public class FunctionableRunner {
    private List<Functionable<?>> functionables; // functionable, topic
    private String baseTopicName = "FUNCTIONABLE_";
    private static FunctionableRunner instance = null;
    private List<MessageConsumer> functionalMessageConsumers;
    private Context serverContext;

    private FunctionableRunner(Context serverContext) {
        instance = new FunctionableRunner(serverContext);
        this.serverContext = serverContext;
    }

    public synchronized static FunctionableRunner getInstance(Context serverContext) {
        if (instance == null) {
            instance = new FunctionableRunner(serverContext);
        }
        return instance;
    }

    /**
     * Start all consumers for the functionables
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void startConsumers() throws ExecutionException, InterruptedException {
        for (MessageConsumer consumer : functionalMessageConsumers) {
            if (consumer != null) {
                consumer.start();
            }
        }
    }

    /**
     * Register and create a functionable instance
     * 
     * @param className   the class name of the functionable class (not the abstract
     *                    Functionable)
     * @param hasConsumer whether this functionable need to communicate with master
     */
    public void registerFunctionable(String className, boolean hasConsumer) {
        try {
            Functionable<?> functionable = getFunctionableInstance(className);

            if (functionable.getClass() == Functionable.class) {
                System.out.println("The functionable class is abstract.");
            } else {
                if (hasConsumer) {
                    functionable.setTopic(baseTopicName + functionables.size());
                    functionalMessageConsumers.add(functionable.getWorkerMessageConsumer());
                }
                functionables.add(functionable);
            }

        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
                | NoSuchMethodException | SecurityException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Register several functionables by class names and defautlt topics
     * 
     * @param classNames   the class names of the functionable classes (not the
     *                     abstract Functionable)
     * @param haveConsumer whether functionables need to communicate with master
     */
    public void registerFunctionables(String[] classNames, boolean[] haveConsumer) {
        assert classNames.length == haveConsumer.length;
        for (int i = 0; i < classNames.length; i++) {
            this.registerFunctionable(classNames[i], haveConsumer[i]);
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
            this.registerFunctionable(classNames[i], false);
        }
    }

    /**
     * To run all functionable tasks
     */
    public void runFunctionableTasks() {
        for (Functionable<?> functionable : functionables) {
            functionable.execute(serverContext);
        }
    }

    /**
     * Initiate a functionable instance
     * 
     * @param className implemented functionable class
     * @return
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     * @throws InvocationTargetException
     * @throws NoSuchMethodException
     * @throws SecurityException
     * @throws ClassNotFoundException
     */
    private static Functionable<?> getFunctionableInstance(String className)
            throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException,
            NoSuchMethodException, SecurityException, ClassNotFoundException {
        Class<?> cls = Class.forName(className);
        Constructor<?> constructor = cls.getConstructor(String.class);
        Functionable<?> functionable = (Functionable) constructor.newInstance();
        return functionable;
    }
}
