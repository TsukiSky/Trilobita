package com.trilobita.engine.server.util.functionable.examples;

import java.io.Serializable;

import com.trilobita.commons.Computable;

public class ExampleFunctionable implements Serializable{
    public String className;
    public String topic;
    public Computable<?> value;

    public ExampleFunctionable(String className, String topic,Computable<?> value){
        this.className = className;
        this.topic = topic;
        this.value = value;
    }

        public ExampleFunctionable(String className, Computable<?> value){
        this.className = className;
        this.value = value;
    }
}
