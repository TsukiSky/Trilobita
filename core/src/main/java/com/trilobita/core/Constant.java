package com.trilobita.core;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class Constant {
    @Value("${kafka.bootstrap.servers}")
    public String KAFKA_BOOTSTRAP_SERVERS;
}
