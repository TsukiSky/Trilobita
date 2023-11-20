package com.trilobita.engine.monitor;

import com.trilobita.engine.monitor.metrics.Metrics;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Monitor is the singleton class to monitor the performance of the system
 */
public class Monitor {
    private Monitor() {}

    /**
     * Start monitoring
     */
    public static void start() {
        Metrics.Overall.startTime = System.nanoTime();
        Metrics.Superstep.initialize();
    }

    /**
     * Stop monitoring
     */
    public static void stop() {
        Metrics.Overall.endTime = System.nanoTime();
    }

    /**
     * Start to record the statistics of a new superstep
     */
    private static void startSuperstep() {
        Metrics.Superstep.reset();
    }

    /**
     * Record the statistics of one superstep
     */
    private static void stopSuperstep() {
        Metrics.Overall.update();
    }

    /**
     * Stop the statistics of one superstep and start to record the statistics of a new superstep
     */
    public static void stopAndStartNewSuperstep() {
        stopSuperstep();
        startSuperstep();
    }

    public static void store(String path) {
        // TODO: implement this method
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(path + "metrics.log"))) {
            writer.write("Mean: " + Metrics.Overall.executionDurations.getMean() + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void store() {
        store("");
    }
}
