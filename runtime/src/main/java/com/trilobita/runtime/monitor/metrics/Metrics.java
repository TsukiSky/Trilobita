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

        public void incrementTime(Long time) {
            SuperstepStatistics.time.addAndGet(time);
        }

        public void incrementMessageNum(Integer increment) {
            SuperstepStatistics.messageNum.addAndGet(increment);
        }

        public void incrementVertexNum(Integer increment) {
            SuperstepStatistics.vertexNum.addAndGet(increment);
        }

        public void incrementEdgeNum(Integer increment) {
            SuperstepStatistics.edgeNum.addAndGet(increment);
        }

        public void resetTimeAndMessageNum() {
            SuperstepStatistics.time.set(0);
            SuperstepStatistics.messageNum.set(0);
        }

        public void initialize() {
            SuperstepStatistics.time.set(0);
            SuperstepStatistics.messageNum.set(0);
            SuperstepStatistics.vertexNum.set(0);
            SuperstepStatistics.edgeNum.set(0);
        }

        public void initialize(int vertexNum, int edgeNum) {
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
        private static DescriptiveStatistics superstepExecutionTimes = new DescriptiveStatistics();
        private static DescriptiveStatistics messageNums = new DescriptiveStatistics();
        private static DescriptiveStatistics vertexNums = new DescriptiveStatistics();
        private static DescriptiveStatistics edgeNums = new DescriptiveStatistics();

        public static void update() {
            superstepExecutionTimes.addValue(SuperstepStatistics.time.doubleValue());
            messageNums.addValue(SuperstepStatistics.messageNum.intValue());
            vertexNums.addValue(SuperstepStatistics.vertexNum.intValue());
            edgeNums.addValue(SuperstepStatistics.edgeNum.intValue());
        }
    }
}
