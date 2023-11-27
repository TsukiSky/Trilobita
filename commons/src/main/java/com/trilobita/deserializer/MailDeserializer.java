package com.trilobita.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trilobita.commons.Mail;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.util.SerializationUtils;

import java.util.Map;

@Slf4j
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

        return (Mail) SerializationUtils.deserialize(data);

    }

    @Override
    public Mail deserialize(String topic, Headers headers, byte[] data) {
        try {
            return (Mail) SerializationUtils.deserialize(data);
        } catch (Exception e){
            return null;
        }
//        return Deserializer.super.deserialize(topic, headers, data);
    }

    @Override
    public void close() {
        Deserializer.super.close();
    }
}
