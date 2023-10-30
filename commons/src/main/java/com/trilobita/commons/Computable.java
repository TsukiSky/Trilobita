package com.trilobita.commons;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Computable<T> implements Serializable {
    protected T value;
    public Computable<T> add(Computable<T> computable) {
        return null;
    }

    public  Computable<T> minus(Computable<T> computable){
        return null;
    };
    public Computable<T> multiply(Computable<T> computable){
        return null;
    };
    public Computable<T> divide(Computable<T> computable){
        return null;
    }
    public T getValue(){
        return null;
    }
    public void setValue(T t){
    }
}
