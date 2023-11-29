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
            writer.write("TotalDurations:\n");
            for (double value : Metrics.Overall.totalDurations.getValues()) {
                writer.write(value/ 1_000_000_000.0 + "\n");
            }

            writer.write("\nDistributionDurations:\n");
            for (double value : Metrics.Overall.distributionDurations.getValues()) {
                writer.write(value/ 1_000_000_000.0 + "\n");
            }

            writer.write("\nExecutionDurations:\n");
            for (double value : Metrics.Overall.executionDurations.getValues()) {
                writer.write(value/ 1_000_000_000.0 + "\n");
            }

            writer.write("\nMessagingDurations:\n");
            for (double value : Metrics.Overall.messagingDurations.getValues()) {
                writer.write(value/ 1_000_000_000.0 + "\n");
            }

            writer.write("\nMessageNums:\n");
            for (double value : Metrics.Overall.messageNums.getValues()) {
                writer.write(value+ "\n");
            }
            writer.write("TotalDurations - Sum: "+ Metrics.Overall.totalDurations.getSum()/ 1_000_000_000.0  + ", Mean: " + Metrics.Overall.totalDurations.getMean()/ 1_000_000_000.0 + ", Min: " + Metrics.Overall.totalDurations.getMin()/ 1_000_000_000.0 + ", Max: " + Metrics.Overall.totalDurations.getMax()/ 1_000_000_000.0 + ", Std Dev: " + Metrics.Overall.totalDurations.getStandardDeviation()/ 1_000_000_000.0 + "\n");
            writer.write("DistributionDurations - Sum: "+Metrics.Overall.distributionDurations.getSum()/ 1_000_000_000.0 + ", Mean: " + Metrics.Overall.distributionDurations.getMean()/ 1_000_000_000.0 + ", Min: " + Metrics.Overall.distributionDurations.getMin()/ 1_000_000_000.0 + ", Max: " + Metrics.Overall.distributionDurations.getMax()/ 1_000_000_000.0 + ", Std Dev: " + Metrics.Overall.distributionDurations.getStandardDeviation()/ 1_000_000_000.0 + "\n");
            writer.write("ExecutionDurations - Sum: "+ Metrics.Overall.executionDurations.getSum()/ 1_000_000_000.0  + ", Mean: " + Metrics.Overall.executionDurations.getMean()/ 1_000_000_000.0 + ", Min: " + Metrics.Overall.executionDurations.getMin()/ 1_000_000_000.0 + ", Max: " + Metrics.Overall.executionDurations.getMax()/ 1_000_000_000.0 + ", Std Dev: " + Metrics.Overall.executionDurations.getStandardDeviation()/ 1_000_000_000.0 + "\n");
            writer.write("MessagingDurations - Sum: "+Metrics.Overall.messagingDurations.getSum()/ 1_000_000_000.0 + ", Mean: " + Metrics.Overall.messagingDurations.getMean()/ 1_000_000_000.0 + ", Min: " + Metrics.Overall.messagingDurations.getMin()/ 1_000_000_000.0 + ", Max: " + Metrics.Overall.messagingDurations.getMax()/ 1_000_000_000.0 + ", Std Dev: " + Metrics.Overall.messagingDurations.getStandardDeviation()/ 1_000_000_000.0 + "\n");
            writer.write("MessageNums - Sum: "+Metrics.Overall.messageNums.getSum()+ ", Mean: " + Metrics.Overall.messageNums.getMean() + ", Min: " + Metrics.Overall.messageNums.getMin() + ", Max: " + Metrics.Overall.messageNums.getMax()+ ", Std Dev: " + Metrics.Overall.messageNums.getStandardDeviation() + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void masterStore(String path) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(path + "metrics.log"))) {
            writer.write("TotalDurations:\n");
            for (double value : Metrics.Overall.totalDurations.getValues()) {
                writer.write(value/ 1_000_000_000.0 + "\n");
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void store() {
        store("");
    }
}
