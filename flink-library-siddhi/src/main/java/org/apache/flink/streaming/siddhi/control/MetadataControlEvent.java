package org.apache.flink.streaming.siddhi.control;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

public class MetadataControlEvent extends AbstractControlEvent {
    private Map<String, String> updatedExecutionPlanMap;
    private Map<String, String> addedExecutionPlanMap;
    private List<String> deletedExecutionPlanId;

    public Map<String, String> getUpdatedExecutionPlanMap() {
        return updatedExecutionPlanMap;
    }

    public MetadataControlEvent setUpdatedExecutionPlanMap(Map<String, String> updatedExecutionPlanMap) {
        this.updatedExecutionPlanMap = updatedExecutionPlanMap;
        return this;
    }

    public List<String> getDeletedExecutionPlanId() {
        return deletedExecutionPlanId;
    }

    public MetadataControlEvent setDeletedExecutionPlanId(List<String> deletedExecutionPlanId) {
        this.deletedExecutionPlanId = deletedExecutionPlanId;
        return this;
    }

    public Map<String, String> getAddedExecutionPlanMap() {
        return addedExecutionPlanMap;
    }

    public MetadataControlEvent setAddedExecutionPlanMap(Map<String, String> addedExecutionPlanMap) {
        this.addedExecutionPlanMap = addedExecutionPlanMap;
        return this;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String getName() {
        return MetadataControlEvent.class.getSimpleName();
    }

    public static class Builder {
        private final MetadataControlEvent metadataControlEvent;

        private Builder() {
            metadataControlEvent = new MetadataControlEvent();
            metadataControlEvent.setAddedExecutionPlanMap(new HashMap<String, String>());
            metadataControlEvent.setUpdatedExecutionPlanMap(new HashMap<String, String>());
            metadataControlEvent.setDeletedExecutionPlanId(new LinkedList<String>());
        }

        public static String nextExecutionPlanId() {
            return UUID.randomUUID().toString();
        }

        public Builder addExecutionPlan(String executionPlan) {
            metadataControlEvent.addedExecutionPlanMap.put(nextExecutionPlanId(), executionPlan);
            return this;
        }

        public Builder addExecutionPlan(String id, String executionPlan) {
            metadataControlEvent.addedExecutionPlanMap.put(id, executionPlan);
            return this;
        }

        public Builder removeExecutionPlan(String id) {
            metadataControlEvent.deletedExecutionPlanId.add(id);
            return this;
        }

        public Builder updateExecutionPlan(String id, String executionPlan) {
            metadataControlEvent.updatedExecutionPlanMap.put(id, executionPlan);
            return this;
        }

        public MetadataControlEvent build() {
            return metadataControlEvent;
        }
    }
}