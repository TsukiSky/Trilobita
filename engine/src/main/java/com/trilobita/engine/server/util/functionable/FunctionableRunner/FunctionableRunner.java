package com.trilobita.engine.server.util.functionable.FunctionableRunner;

import java.util.ArrayList;
import java.util.List;

import com.trilobita.engine.server.util.functionable.Functionable;

import lombok.Data;

/**
 * An FunctionableRunner is a singleton in one worker.
 * It is used to register, initiate, and run functionable instances added by the
 * user.
 */
@Data
public abstract class FunctionableRunner {
    private List<Functionable<?>> functionables = new ArrayList<>(); // functionable, topic

    public Functionable<?> findFunctionableByName(String name) {
        if (this.functionables != null) {
            for (Functionable<?> functionable : this.functionables) {
                if (functionable.instanceName.equals(name)) {
                    return functionable;
                }
            }
        }
        return null;
    }

    public void registerFunctionable(Functionable<?> functionable, String topicName) {
        if (functionable.getClass() == Functionable.class) {
            System.out.println("The functionable class is abstract.");
        } else {
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
