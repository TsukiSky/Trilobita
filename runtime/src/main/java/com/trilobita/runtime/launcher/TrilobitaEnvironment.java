package com.trilobita.runtime.launcher;

import com.trilobita.runtime.configuration.Configuration;
import com.trilobita.runtime.configuration.JCommandHandler;
import com.trilobita.runtime.launcher.inputparser.Parse;
import lombok.Setter;

/**
 * The running environment for Trilobita Job
 */
public class TrilobitaEnvironment {
    public static TrilobitaEnvironment trilobitaEnvironment;
    @Setter
    private Parse inputParser;
    private final Configuration configuration = new Configuration();
    private final JCommandHandler jCommandHandler = new JCommandHandler();  // Command-line handler for Trilobita

    private TrilobitaEnvironment() {
    }

    /**
     * Get the singleton instance of TrilobitaEnvironment
     * @return the singleton instance of TrilobitaEnvironment
     */
    public static TrilobitaEnvironment getTrilobitaEnvironment() {
        if (trilobitaEnvironment == null) {
            trilobitaEnvironment = new TrilobitaEnvironment();
        }
        return trilobitaEnvironment;
    }

    /**
     * Initialize the configuration of Trilobita
     */
    public void initConfig() {
        jCommandHandler.initConfig(configuration);
    }
}
