package com.trilobita.runtime.monitor;

import com.trilobita.runtime.monitor.metrics.Metrics;

/**
 * Monitor is the singleton class to monitor the performance of the system
 */
public class Monitor {
    private Monitor() {}

    /**
     * Start monitoring
     */
    public static void startMonitor() {
        Metrics.OverallStatistics.startTime = System.nanoTime();
    }

    /**
     * Stop monitoring
     */
    public static void stopMonitor() {
        Metrics.OverallStatistics.startTime = System.nanoTime();
    }

    /**
     * Start to record the statistics of a new superstep
     */
    public static void startNewSuperstep() {
        Metrics.SuperstepStatistics.resetTimeAndMessageNum();
    }

    /**
     * Record the statistics of one superstep
     */
    public static void endSuperstep() {
        Metrics.OverallStatistics.update();
    }
}
