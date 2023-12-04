package com.trilobita.engine.server.util.functionable;

import com.trilobita.core.common.Computable;
import com.trilobita.core.common.Mail;
import com.trilobita.core.common.Message;

public class FunctionalMail extends Mail {
    public FunctionalMail(String instanceName, Computable<?> value) {
        super(-1, new Message(new Functionable.FunctionableRepresenter(instanceName, value)),Mail.MailType.FUNCTIONAL);
    }
}
