/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.pinot.committer;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Global committable references all data files that will be committed during checkpointing.
 */
@Internal
public class PinotSinkGlobalCommittable implements Serializable {
    private final List<String> dataFilePaths;
    private final long minTimestamp;
    private final long maxTimestamp;

    /**
     * @param dataFilePaths List of paths to data files on shared file system
     * @param minTimestamp  Minimum timestamp of all objects in all data files
     * @param maxTimestamp  Maximum timestamp of all objects in all data files
     */
    public PinotSinkGlobalCommittable(List<String> dataFilePaths, long minTimestamp, long maxTimestamp) {
        this.dataFilePaths = checkNotNull(dataFilePaths);
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
    }

    public List<String> getDataFilePaths() {
        return dataFilePaths;
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }
}
