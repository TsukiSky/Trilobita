package com.trilobita.runtime.parser.inputparser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trilobita.core.graph.Graph;

import java.io.File;
import java.io.IOException;

/**
 * JSON file input parser
 */
public class JsonParser implements InputParse {
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
