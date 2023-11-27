package com.trilobita.commons;

import java.io.Serializable;

public interface Computable<T> extends Comparable<T>, Serializable{
    public Computable<T> add(Computable<T> computable);
    public  Computable<T> minus(Computable<T> computable);
    public Computable<T> multiply(Computable<T> computable);
    public Computable<T> divide(Computable<T> computable);
    public T getValue();
    public void setValue(T t);
}
