package com.trilobita.server.scheduler;

/**
 * Abstract Scheduler class for all scheduler instances
 */
public abstract class AbstractScheduler {
    private Integer nthreads;   // number of threads controlled by the scheduler

    protected AbstractScheduler(Integer nthreads) {
        this.nthreads = nthreads;
    }
}
