package com.trilobita.engine.server.functionable;

import org.apache.commons.math3.util.Pair;

import com.trilobita.commons.Computable;
import com.trilobita.commons.Mail;
import com.trilobita.commons.Message;

public class FunctionalMail extends Mail {
    public FunctionalMail(String instanceName, Computable<?> value) {
        super(-1, new Message(new Pair<String, Computable<?>>(instanceName, value)), Mail.MailType.FUNCTIONAL);
    }
}
