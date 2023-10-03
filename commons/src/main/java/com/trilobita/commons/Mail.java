package com.trilobita.commons;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Mail {
    private int fromVertexId;
    private int toVertexId;
    private Message<?> message;
    private MailType mailType;
}
