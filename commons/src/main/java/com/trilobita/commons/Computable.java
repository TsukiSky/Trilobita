package com.trilobita.commons;

public interface Computable<T> {
    public Computable<T> add(Computable<T> computable);
    public Computable<T> minus(Computable<T> computable);
    public Computable<T> multiply(Computable<T> computable);
}
