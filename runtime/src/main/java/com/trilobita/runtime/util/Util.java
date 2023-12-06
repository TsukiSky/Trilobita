package com.trilobita.runtime.util;

import com.trilobita.core.graph.Graph;

import java.io.File;
import java.io.IOException;

/**
 * Utility class
 */
public class Util {
    /**
     * Get the number of cores on the current machine
     * @return int
     */
    public static int getCoreNum() {
        return Runtime.getRuntime().availableProcessors();
    }

    public interface GraphLoader {
        <T> Graph<T> loadGraph();
    }
}
