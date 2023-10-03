package com.trilobita.engine.computing.task;

import lombok.Data;

/**
 * Task is the smallest schedulable unit in a Server
 */
@Data
public abstract class Task implements Runnable {
    private int id;
    private TaskStatus status;
    private TaskType type;

    public enum TaskStatus{
        RUNNING, SUCCEEDED, FAILED, RECOVERING
    }

    public enum TaskType {
        VERTEX, MESSAGING, HEARTBEAT
    }
}
