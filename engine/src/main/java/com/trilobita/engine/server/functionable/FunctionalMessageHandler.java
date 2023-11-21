package com.trilobita.engine.server.functionable;

import com.trilobita.commons.Message;

// a handler on master to handle received functinal messages
public interface FunctionalMessageHandler {
    void handleMessage( Message message);
}
