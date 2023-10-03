package com.trilobita.commons;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
public class Mail {
    private int fromVertexId;
    private int toVertexId;
    private List<Message<?>> messages;
    private MailType mailType;

    public void add(Message<?> message){
        messages.add(message);
    }
}



