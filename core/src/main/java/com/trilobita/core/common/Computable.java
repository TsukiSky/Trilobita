package com.trilobita.core.common;

import java.io.Serializable;

public interface Computable<T> extends Comparable<T>, Serializable{
    Computable<T> add(Computable<T> computable);
    Computable<T> minus(Computable<T> computable);
    Computable<T> multiply(Computable<T> computable);Computable<T> divide(Computable<T> computable);
    T getValue();
    void setValue(T t);
}
