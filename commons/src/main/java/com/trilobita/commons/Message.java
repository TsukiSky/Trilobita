package com.trilobita.commons;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class Message<T> implements Combinable{
    private T content;
    private MessageType messageType;

    @Override
    public Message<?> combine(Message<?> message){
        return this;
    }
}
