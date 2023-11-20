package com.trilobita.engine.server.functionable;

import java.util.concurrent.CopyOnWriteArrayList;

import com.trilobita.commons.Mail;

// a handler on master to handle received functinal messages
public interface FunctionalMailsHandler {
    void handleMails( CopyOnWriteArrayList<Mail> mails);
}
