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

    @Override
    public boolean equals(Object o){
        if (o == this){
            return true;
        }
        if (!(o instanceof Mail)){
            return false;
        }
        Mail mail = (Mail) o;
        return this.fromVertexId==mail.fromVertexId && this.toVertexId==mail.toVertexId
                && this.message==mail.message && this.mailType==mail.mailType;
    }
}



