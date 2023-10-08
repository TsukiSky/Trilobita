package com.trilobita.commons;

public interface Computable<T> {
    public T add(Computable<?> computable);
    public T minus(Computable<T> computable);
    public T multiple(T computable);
}
