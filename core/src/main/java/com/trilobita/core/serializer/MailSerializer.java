package com.trilobita.core.serializer;

import com.trilobita.core.common.Mail;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

@Slf4j
public class MailSerializer implements Serializer<Mail> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }

    @Override
    public void close() {
        Serializer.super.close();
    }

    @Override
    public byte[] serialize(String s, Mail mail) {
        org.apache.commons.lang3.SerializationUtils.serialize(mail);
        return SerializationUtils.serialize(mail);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Mail data) {
        return SerializationUtils.serialize(data);
//        return Serializer.super.serialize(topic, headers, data);
    }

}
