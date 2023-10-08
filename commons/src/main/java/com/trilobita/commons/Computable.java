package com.trilobita.commons;

public interface Computable<T> {
    Computable<T> combine(Computable<T> content);
}
