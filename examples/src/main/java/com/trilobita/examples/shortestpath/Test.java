package com.trilobita.examples.shortestpath;

import com.trilobita.core.messaging.MessageAdmin;

import java.util.concurrent.ExecutionException;

public class Test {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        MessageAdmin.getInstance().purgeAllTopics();
    }
}
