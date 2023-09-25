package com.trilobita.core.messaging;

import lombok.extern.slf4j.Slf4j;

public class TestDrive {
    public static void main(String[] args) {
        Producer.produce("msg2SvrB", "hiSvrB", "topic_0");
    }
}
