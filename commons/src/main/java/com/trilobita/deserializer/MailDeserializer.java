package com.trilobita.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trilobita.commons.Mail;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class MailDeserializer implements Deserializer<Mail> {
    private final ObjectMapper objectMapper = new ObjectMapper();


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Deserializer.super.configure(configs, isKey);
    }

    @Override
    public Mail deserialize(String s, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.readValue(data, Mail.class);
        } catch (IOException e) {
            throw new SerializationException("Error deserializing Mail object", e);
        }
    }

    @Override
    public Mail deserialize(String topic, Headers headers, byte[] data) {
        return Deserializer.super.deserialize(topic, headers, data);
    }

    @Override
    public void close() {
        Deserializer.super.close();
    }
}
