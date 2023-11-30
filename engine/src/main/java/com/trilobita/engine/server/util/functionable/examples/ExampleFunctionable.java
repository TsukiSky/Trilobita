package com.trilobita.engine.server.util.functionable.examples;

import java.io.Serializable;

import com.trilobita.commons.Computable;

public class ExampleFunctionable implements Serializable{
    public String className;
    public String topic;
    public Computable<?> initLastValue;
    public Computable<?> initNewValue;

    public ExampleFunctionable(String className, String topic,Computable<?> initLastValue,Computable<?> initNewValue){
        this.className = className;
        this.topic = topic;
        this.initLastValue = initLastValue;
        this.initNewValue = initNewValue;
    }

    public ExampleFunctionable(String className, String topic,Computable<?> initLastValue){
        this.className = className;
        this.topic = topic;
        this.initLastValue = initLastValue;
        this.initNewValue = initLastValue;
    }

    public ExampleFunctionable(String className, Computable<?> initLastValue,Computable<?> initNewValue){
        this.className = className;
        this.initLastValue = initLastValue;
        this.initNewValue = initNewValue;
    }

    public ExampleFunctionable(String className, Computable<?> initLastValue){
        this.className = className;
        this.initLastValue = initLastValue;
        this.initNewValue = initLastValue;
    }
}
