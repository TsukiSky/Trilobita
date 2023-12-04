package com.trilobita.core;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class Util {
    private Util() {}
    public static Properties loadConfig(final String configFile) {
        if (!Files.exists(Paths.get(configFile))) {
            return new Properties();
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
        } catch (IOException e) {
            return new Properties();
        }
        return cfg;
    }
}
