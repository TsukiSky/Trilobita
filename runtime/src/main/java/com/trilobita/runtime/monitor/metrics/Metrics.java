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
    public static class SuperstepStats {
        private static AtomicLong time;
        private static AtomicInteger messageNum;
        private static AtomicInteger vertexNum;
        private static AtomicInteger edgeNum;

        public void incrementTime(Long time) {
            SuperstepStats.time.addAndGet(time);
        }

        public void incrementMessageNum(Integer increment) {
            SuperstepStats.messageNum.addAndGet(increment);
        }

        public void incrementvertexNum(Integer increment) {
            SuperstepStats.vertexNum.addAndGet(increment);
        }

        public void incrementedgeNum(Integer increment) {
            SuperstepStats.edgeNum.addAndGet(increment);
        }

        public void resetTimeAndMessageNum() {
            SuperstepStats.time.set(0);
            SuperstepStats.messageNum.set(0);
        }

        public void initialize() {
            SuperstepStats.time.set(0);
            SuperstepStats.messageNum.set(0);
            SuperstepStats.vertexNum.set(0);
            SuperstepStats.edgeNum.set(0);
        }

        public void initialize(int vertexNum, int edgeNum) {
            SuperstepStats.time.set(0);
            SuperstepStats.messageNum.set(0);
            SuperstepStats.vertexNum.set(vertexNum);
            SuperstepStats.edgeNum.set(edgeNum);
        }
    }

    /**
     * Overall performance of the system
     */
    @Data
    public static class OverallStats {
        private static DescriptiveStatistics superstepExecutionTimes = new DescriptiveStatistics();
        private static DescriptiveStatistics messageNums = new DescriptiveStatistics();
        private static DescriptiveStatistics vertexNums = new DescriptiveStatistics();
        private static DescriptiveStatistics edgeNums = new DescriptiveStatistics();

        public static void update() {
            superstepExecutionTimes.addValue(SuperstepStats.time.doubleValue());
            messageNums.addValue(SuperstepStats.messageNum.intValue());
            vertexNums.addValue(SuperstepStats.vertexNum.intValue());
            edgeNums.addValue(SuperstepStats.edgeNum.intValue());
        }
    }

}
