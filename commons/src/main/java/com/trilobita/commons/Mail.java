package com.trilobita.commons;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@EqualsAndHashCode
public class Mail {
    private int fromVertexId;
    private int toVertexId;
    private Message<?> message;
    private MailType mailType;

    public Mail(int toVertexId, Message<?> message, MailType mailType) {
        this.toVertexId = toVertexId;
        this.message = message;
        this.mailType = mailType;
        this.fromVertexId = 0;
    }
}
