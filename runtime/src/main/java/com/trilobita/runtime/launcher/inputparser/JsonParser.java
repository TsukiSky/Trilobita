package com.trilobita.runtime.launcher.inputparser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trilobita.core.graph.Graph;

import java.io.File;
import java.io.IOException;

/**
 * JSON file input parser
 */
public class JsonParser implements Parse {
    @Override
    public Graph parse(File file) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readValue(file, Graph.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
