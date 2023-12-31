package com.trilobita.engine.server.util.functionable.functionableRunner;

import java.util.ArrayList;
import java.util.List;

import com.trilobita.engine.server.util.functionable.Functionable;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * An FunctionableRunner is a singleton in one worker.
 * It is used to register, initiate, and run functionable instances added by the
 * user.
 */
@Data
@Slf4j
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

    public void registerFunctionable(Functionable<?> functionable) {
        if (!this.getFunctionables().contains(functionable)) {
            this.getFunctionables().add(functionable);
        } else {
            log.info("Functionable {} already exists.", functionable);
        }
    }
}
