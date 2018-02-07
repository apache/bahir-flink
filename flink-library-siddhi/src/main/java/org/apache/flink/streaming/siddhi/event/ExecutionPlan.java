package org.apache.flink.streaming.siddhi.event;

import java.io.Serializable;

public class ExecutionPlan implements Serializable {
    private String id;
    private String executionPlan;

    public String getExecutionPlan() {
        return executionPlan;
    }

    public ExecutionPlan setExecutionPlan(String executionPlan) {
        this.executionPlan = executionPlan;
        return this;
    }

    public String getId() {
        return id;
    }

    public ExecutionPlan setId(String id) {
        this.id = id;
        return this;
    }
}