package com.trilobita.runtime.monitor;

/**
 * Monitor is the singleton class to monitor the performance of the system
 */
public class Monitor {
    private static Monitor instance;

    private Monitor() {}

    /**
     * Get the singleton instance of Monitor
     * @return the singleton instance of Monitor
     */
    public static Monitor getMonitor() {
        if (instance == null) {
            instance = new Monitor();
        }
        return instance;
    }
}
