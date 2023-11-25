package com.trilobita.runtime.util;

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
}
