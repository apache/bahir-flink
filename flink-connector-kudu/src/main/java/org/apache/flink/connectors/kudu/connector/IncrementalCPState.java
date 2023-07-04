/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.connectors.kudu.connector;


import org.apache.flink.annotation.Internal;

import java.io.Serializable;

@Internal
public class IncrementalCPState implements Serializable {
    private static final long serialVersionUID = 1L;

    private int subTaskId;
    private long checkpointId;
    private String offset;
    private boolean committed;

    public IncrementalCPState(int subTaskId, long checkpointId, String offset, boolean committed) {
        this.subTaskId = subTaskId;
        this.checkpointId = checkpointId;
        this.offset = offset;
        this.committed = committed;
    }

    public int getSubTaskId() {
        return subTaskId;
    }

    public String getOffset() {
        return offset;
    }

    public boolean isCommitted() {
        return committed;
    }

    public void setCommitted(boolean committed) {
        this.committed = committed;
    }

    public static IncrementalCPState.Builder builder() {
        return new IncrementalCPState.Builder();
    }

    @Override
    public String toString() {
        return "IncrementalCPState{" +
                "subTaskId=" + subTaskId +
                ", checkpointId=" + checkpointId +
                ", offset='" + offset + '\'' +
                ", committed=" + committed +
                '}';
    }

    public static class Builder {
        private int subTaskId;
        private long checkpointId;
        private String offset;
        private boolean committed;
        public Builder subTaskId(int suTaskId) {
            this.subTaskId = suTaskId;
            return this;
        }

        public Builder checkpointId(long checkpointId) {
            this.checkpointId = checkpointId;
            return this;
        }

        public Builder offset(String offset) {
            this.offset = offset;
            return this;
        }

        public Builder committed(boolean committed) {
            this.committed = committed;
            return this;
        }

        public IncrementalCPState build() {
            return new IncrementalCPState(subTaskId, checkpointId, offset, committed);
        }
    }
}
