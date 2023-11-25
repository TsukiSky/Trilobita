package com.trilobita.engine.server.util.functionable;

import com.trilobita.commons.Computable;
import com.trilobita.commons.Mail;
import com.trilobita.commons.Message;
import com.trilobita.engine.server.util.functionable.examples.ExampleFunctionable;

public class FunctionalMail extends Mail {
    public FunctionalMail(String instanceName, Computable<?> value) {
        super(-1, new Message(new ExampleFunctionable(instanceName, value)),Mail.MailType.FUNCTIONAL);
    }
}
