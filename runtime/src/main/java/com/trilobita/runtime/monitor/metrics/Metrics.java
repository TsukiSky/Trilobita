package com.trilobita.runtime.monitor.metrics;

import lombok.Data;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Metrics is used to record the system's performance during execution
 */
public class Metrics {
    /**
     * Statistics recorded in one superstep
     */
    @Data
    public static class SuperstepStatistics {
        private static AtomicLong time;
        private static AtomicInteger messageNum;
        private static AtomicInteger vertexNum;
        private static AtomicInteger edgeNum;

        /**
         * Increment the time
         * @param time the time to be incremented, unit is nanosecond
         */
        public static void incrementTime(Long time) {
            SuperstepStatistics.time.addAndGet(time);
        }

        /**
         * Increment the number of messages
         * @param increment the number of messages to be incremented
         */
        public static void incrementMessageNum(Integer increment) {
            SuperstepStatistics.messageNum.addAndGet(increment);
        }

        /**
         * Increment the number of vertices
         * @param increment the number of vertices to be incremented
         */
        public static void incrementVertexNum(Integer increment) {
            SuperstepStatistics.vertexNum.addAndGet(increment);
        }

        /**
         * Increment the number of edges
         * @param increment the number of edges to be incremented
         */
        public static void incrementEdgeNum(Integer increment) {
            SuperstepStatistics.edgeNum.addAndGet(increment);
        }

        public static void resetTimeAndMessageNum() {
            SuperstepStatistics.time.set(0);
            SuperstepStatistics.messageNum.set(0);
        }

        public static void initialize() {
            SuperstepStatistics.time.set(0);
            SuperstepStatistics.messageNum.set(0);
            SuperstepStatistics.vertexNum.set(0);
            SuperstepStatistics.edgeNum.set(0);
        }

        public static void initialize(int vertexNum, int edgeNum) {
            SuperstepStatistics.time.set(0);
            SuperstepStatistics.messageNum.set(0);
            SuperstepStatistics.vertexNum.set(vertexNum);
            SuperstepStatistics.edgeNum.set(edgeNum);
        }
    }

    /**
     * Overall performance of the system
     */
    @Data
    public static class OverallStatistics {
        public static long startTime;
        public static long endTime;
        public static DescriptiveStatistics superstepExecutionTimes = new DescriptiveStatistics();
        public static DescriptiveStatistics messageNums = new DescriptiveStatistics();

        /**
         * Update the overallStatistics
         */
        public static void update() {
            superstepExecutionTimes.addValue(SuperstepStatistics.time.doubleValue());
            messageNums.addValue(SuperstepStatistics.messageNum.intValue());
//            vertexNums.addValue(SuperstepStatistics.vertexNum.intValue());
//            edgeNums.addValue(SuperstepStatistics.edgeNum.intValue());
        }
    }
}
