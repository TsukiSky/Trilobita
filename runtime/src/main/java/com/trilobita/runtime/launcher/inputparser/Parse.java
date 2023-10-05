package com.trilobita.runtime.launcher.inputparser;

import com.trilobita.core.graph.Graph;

import java.io.File;

/**
 * Parse interface indicate the function of parsing input to a Graph
 */
public interface Parse {
    Graph parse(File file);
}
