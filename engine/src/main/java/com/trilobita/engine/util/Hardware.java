package com.trilobita.engine.util;

/**
 * Hardware class contains all necessary methods to get the current hardware's information
 */
public class Hardware {
    /**
     * Get the number of cores on the current machine
     * @return int
     */
    public static int getCoreNum() {
        return Runtime.getRuntime().availableProcessors();
    }
}
