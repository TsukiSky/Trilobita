package com.trilobita.engine.monitor.metrics;

import lombok.Data;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Metrics is used to record the system's performance during execution
 */
public class Metrics {
    /**
     * Statistics recorded in one superstep
     */
    @Data
    public static class Superstep {
        private static long distributionStartTime;
        private static long distributionEndTime;
        private static long distributionDuration;

        private static long executionStartTime;
        private static long executionEndTime;
        private static long executionDuration;

        private static long messagingStartTime;
        private static long messagingEndTime;
        private static long messagingDuration;

        private static long startTime;
        private static long endTime;
        private static long duration;

        private static AtomicInteger messageNum;
        private static AtomicInteger vertexNum;
        private static AtomicInteger edgeNum;

        /**
         * Increment the number of messages
         * @param increment the number of messages to be incremented
         */
        public static void incrementMessageNum(Integer increment) {
            Superstep.messageNum.addAndGet(increment);
        }

        /**
         * Increment the number of vertices
         * @param increment the number of vertices to be incremented
         */
        public static void incrementVertexNum(Integer increment) {
            Superstep.vertexNum.addAndGet(increment);
        }

        /**
         * Increment the number of edges
         * @param increment the number of edges to be incremented
         */
        public static void incrementEdgeNum(Integer increment) {
            Superstep.edgeNum.addAndGet(increment);
        }

        public static void reset() {
            Superstep.distributionDuration = 0;
            Superstep.executionDuration = 0;
            Superstep.messagingDuration = 0;
            Superstep.messageNum.set(0);
        }

        public static void setSuperstepStartTime() {
            startTime = System.nanoTime();
        }
        public static void setMessagingStartTime(){
            messagingStartTime = System.nanoTime();
        }
        public static void setDistributionStartTime(){
            distributionStartTime = System.nanoTime();
        }
        public static void setExecutionStartTime(){

            executionStartTime = System.nanoTime();
        }

        public static void
        initialize() {
            Superstep.messageNum = new AtomicInteger(0);
            Superstep.vertexNum = new AtomicInteger(0);
            Superstep.edgeNum = new AtomicInteger(0);
            Superstep.reset();
        }

        public static void initialize(int vertexNum, int edgeNum) {
            Superstep.initialize();
            Superstep.reset();
            Superstep.vertexNum.set(vertexNum);
            Superstep.edgeNum.set(edgeNum);
        }

        public static void computeDistributionDuration(){
            distributionEndTime = System.nanoTime();
            distributionDuration = distributionEndTime - distributionStartTime;
        }

        public static void computeExecutionDuration(){
            executionEndTime = System.nanoTime();
            executionDuration = executionEndTime - executionStartTime;
        }

        public static void computeMessagingDuration(){
            messagingEndTime = System.nanoTime();
            messagingDuration = messagingEndTime - messagingStartTime;
        }

        public static void computeSuperstepDuration() {
            endTime = System.nanoTime();
            duration = endTime - startTime;
        }

    }

    /**
     * Overall performance of the system
     */
    @Data
    public static class Overall {
        public static long startTime;
        public static long endTime;
        public static DescriptiveStatistics totalDurations = new DescriptiveStatistics();
        public static DescriptiveStatistics distributionDurations = new DescriptiveStatistics();
        public static DescriptiveStatistics executionDurations = new DescriptiveStatistics();
        public static DescriptiveStatistics messagingDurations = new DescriptiveStatistics();
        public static DescriptiveStatistics messageNums = new DescriptiveStatistics();

        /**
         * Update the overallStatistics
         */
        public static void update() {
            totalDurations.addValue(Superstep.duration);
            distributionDurations.addValue(Superstep.distributionDuration);
            executionDurations.addValue(Superstep.executionDuration);
            messagingDurations.addValue(Superstep.messagingDuration);
            messageNums.addValue(Superstep.messageNum.intValue());
        }
    }


}
