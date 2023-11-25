package com.trilobita.engine.server.util.functionable.examples;

import java.io.Serializable;

import com.trilobita.commons.Computable;

public class ExampleFunctionable implements Serializable{
    public String className;
    public String topic;
    public Computable<?> initValue;

    public ExampleFunctionable(String className, String topic,Computable<?> initValue){
        this.className = className;
        this.topic = topic;
        this.initValue = initValue;
    }

        public ExampleFunctionable(String className, Computable<?> initValue){
        this.className = className;
        this.initValue = initValue;
    }
}
