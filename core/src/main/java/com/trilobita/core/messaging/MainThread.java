package com.trilobita.core.messaging;

import com.trilobita.core.Constant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MainThread extends Thread {
    @Autowired
    Constant constant;

    @Override
    public void run() {
        System.out.println();
    }
}
