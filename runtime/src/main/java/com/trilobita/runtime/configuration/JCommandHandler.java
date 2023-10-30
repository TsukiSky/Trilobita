package com.trilobita.runtime.configuration;

import com.beust.jcommander.Parameter;

/**
 * Command-line handler for Trilobita
 */
public class JCommandHandler {
    @Parameter(names = {"--help", "-h"}, description = "Print this help message")
    public boolean help = false;
    @Parameter(names = {"--parallelism"}, description = "The parallelism of the job")
    public int parallelism = 4; // default parallelism

    /**
     * Load the properties from the JCommander
     * @param configuration the configuration of Trilobita
     */
    public void initConfig(Configuration configuration) {
        configuration.put("parallelism", parallelism);
    }
}
