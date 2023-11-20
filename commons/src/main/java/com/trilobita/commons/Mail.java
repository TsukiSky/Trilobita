package com.trilobita.commons;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Mail serves as the message payload
 */
@Data
@NoArgsConstructor
@EqualsAndHashCode
public class Mail implements Serializable {
    private int fromServerId;
    private int fromVertexId;
    private int toVertexId;
    private Message message;
    private MailType mailType;

    public Mail(int toVertexId, Message message, MailType mailType) {
        this.toVertexId = toVertexId;
        this.message = message;
        this.mailType = mailType;
        this.fromVertexId = 0;
    }

    public Mail(int fromVertexId, int toVertexId, Message message, MailType mailType) {
        this.fromVertexId = fromVertexId;
        this.toVertexId = toVertexId;
        this.message = message;
        this.mailType = mailType;
        this.fromServerId = -1;
    }

    public Mail(int fromServerId, int fromVertexId, int toVertexId, Message message, MailType mailType) {
        this.fromServerId = fromServerId;
        this.fromVertexId = fromVertexId;
        this.toVertexId = toVertexId;
        this.message = message;
        this.mailType = mailType;
    }

    /**
     * MailType is used to distinguish different types of mails
     */
    public enum MailType {
        NORMAL,
        PARTITION,
        FINISH_SIGNAL,
        START_SIGNAL,
        FUNCTIONAL,
        BROADCAST 
    }
}
