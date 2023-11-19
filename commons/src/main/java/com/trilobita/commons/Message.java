package com.trilobita.commons;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
public class Message implements Serializable {
    private Object content;
    private MessageType messageType;

    public enum MessageType {
        BARRIER,
        NORMAL,
        NULL,
        BROADCAST
    }
}
