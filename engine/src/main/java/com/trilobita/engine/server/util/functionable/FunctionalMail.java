package com.trilobita.engine.server.util.functionable;

import com.trilobita.commons.Computable;
import com.trilobita.commons.Mail;
import com.trilobita.commons.Message;

public class FunctionalMail extends Mail {
    public FunctionalMail(String instanceName, Computable<?> value) {
        super(-1, new Message(new Functionable.FunctionableRepresenter(instanceName, value)),Mail.MailType.FUNCTIONAL);
    }
}
