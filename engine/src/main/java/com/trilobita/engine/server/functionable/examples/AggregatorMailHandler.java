package com.trilobita.engine.server.functionable.examples;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trilobita.commons.Mail;
import com.trilobita.engine.server.functionable.FunctionalMailHandler;

public class AggregatorMailHandler implements FunctionalMailHandler {

    @Override
    public void handleMessage(UUID key, Mail value, int partition, long offset)
            throws JsonProcessingException, InterruptedException, ExecutionException {
        // TODO Auto-generated method stub

        
    }


}
