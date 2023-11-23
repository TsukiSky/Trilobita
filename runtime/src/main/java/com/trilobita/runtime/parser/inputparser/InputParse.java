package com.trilobita.runtime.parser.inputparser;

import com.trilobita.core.graph.Graph;

import java.io.File;

/**
 * Parse interface indicate the function of parsing input to a Graph
 */
public interface InputParse {
    Graph parse(File file);
}
