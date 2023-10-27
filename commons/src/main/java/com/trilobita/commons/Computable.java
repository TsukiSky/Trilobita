package com.trilobita.commons;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Computable<T> {
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
    };
    public T get(){
        return null;
    };
    public void set(T t){
    };
}
