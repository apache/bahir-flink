/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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