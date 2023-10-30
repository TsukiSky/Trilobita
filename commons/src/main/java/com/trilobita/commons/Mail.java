package com.trilobita.commons;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@EqualsAndHashCode
public class Mail{
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

    @JsonCreator
    public Mail(@JsonProperty("fromVertexId") int fromVertexId,
                @JsonProperty("toVertexId") int toVertexId,
                @JsonProperty("message") Message message,
                @JsonProperty("mailType") MailType mailType) {
        this.fromVertexId = fromVertexId;
        this.toVertexId = toVertexId;
        this.message = message;
        this.mailType = mailType;
    }

    public enum MailType {
        NORMAL,
        GRAPH_PARTITION,
        FINISH_INDICATOR,
    }
}
