package com.trilobita.core.exception;

public class TrilobitaException extends Exception {
    public TrilobitaException(String message) {
        super(message);
    }

    public TrilobitaException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
