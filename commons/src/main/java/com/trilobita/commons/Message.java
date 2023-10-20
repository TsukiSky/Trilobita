package com.trilobita.commons;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@AllArgsConstructor
@EqualsAndHashCode
public class Message<T>{
    private Computable<T> content;
    private MessageType messageType;
}
