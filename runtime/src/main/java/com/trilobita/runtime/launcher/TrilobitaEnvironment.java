package com.trilobita.runtime.launcher;

import com.trilobita.runtime.launcher.inputparser.Parse;

/**
 * The running environment for Trilobita Job
 */
public class TrilobitaEnvironment {
    public static TrilobitaEnvironment trilobitaEnvironment;
    private Parse inputParser;

    private TrilobitaEnvironment() {
    }

    public static TrilobitaEnvironment getTrilobitaEnvironment() {
        if (trilobitaEnvironment == null) {
            trilobitaEnvironment = new TrilobitaEnvironment();
        }
        return trilobitaEnvironment;
    }
}
