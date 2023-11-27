package com.trilobita.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trilobita.commons.Mail;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.util.SerializationUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

@Slf4j
public class MailSerializer implements Serializer<Mail> {

    private final ObjectMapper objectMapper = new ObjectMapper();


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String s, Mail mail) {
        return SerializationUtils.serialize(mail);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Mail data) {
        return SerializationUtils.serialize(data);
//        return Serializer.super.serialize(topic, headers, data);
    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}