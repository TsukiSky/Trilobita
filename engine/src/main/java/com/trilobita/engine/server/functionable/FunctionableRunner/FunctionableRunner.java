package com.trilobita.engine.server.functionable.FunctionableRunner;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.trilobita.commons.Computable;
import com.trilobita.core.messaging.MessageConsumer;
import com.trilobita.core.messaging.MessageConsumer.MessageHandler;
import com.trilobita.engine.server.Context;
import com.trilobita.engine.server.functionable.Functionable;

import lombok.Data;

/**
 * An FunctionableRunner is a singleton in one worker.
 * It is used to register, initiate, and run functionable instances added by the
 * user.
 */
@Data
public abstract class FunctionableRunner {
    private List<Functionable<?>> functionables; // functionable, topic

    public Functionable<?> findFunctionableByName(String name){
        for (Functionable<?> functionable : this.functionables) {
            if (functionable.instanceName == name) {
                return functionable;
            }
        }
        return null;
    }

    public void registerFunctionable(Functionable<?> functionable, String topicName) {
        if (functionable.getClass() == Functionable.class) {
            System.out.println("The functionable class is abstract.");
        } else {
            if (topicName != null) {
                functionable.setTopic(topicName);
            }
            this.getFunctionables().add(functionable);
        }
    }

        public void registerFunctionable(Functionable<?> functionable) {
        if (functionable.getClass() == Functionable.class) {
            System.out.println("The functionable class is abstract.");
        } else {
            this.getFunctionables().add(functionable);
        }
    }
}
