package com.trilobita.engine.computing.job;

/**
 * Job is the entity to be run in the system
 */
public abstract class Job {
    private int id;
    private JobStatus status;

    public enum JobStatus {
        RUNNING, SUCCEEDED, FAILED, RECOVERING
    }
}
