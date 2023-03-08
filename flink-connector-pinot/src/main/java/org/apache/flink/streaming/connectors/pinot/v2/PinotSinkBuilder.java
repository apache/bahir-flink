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

package org.apache.flink.streaming.connectors.pinot.v2;

import org.apache.flink.streaming.connectors.pinot.filesystem.FileSystemAdapter;
import org.apache.flink.streaming.connectors.pinot.segment.name.SimpleSegmentNameGenerator;
import org.apache.flink.streaming.connectors.pinot.v2.committer.PinotSinkCommitter;
import org.apache.flink.streaming.connectors.pinot.v2.external.EventTimeExtractor;
import org.apache.flink.streaming.connectors.pinot.v2.external.JsonSerializer;
import org.apache.pinot.core.segment.name.SegmentNameGenerator;

public class PinotSinkBuilder<IN> {

    static final int DEFAULT_COMMIT_THREADS = 4;

    String pinotControllerHost;
    String pinotControllerPort;
    String tableName;
    int maxRowsPerSegment;
    String tempDirPrefix = "flink-connector-pinot";
    JsonSerializer<IN> jsonSerializer;
    EventTimeExtractor<IN> eventTimeExtractor;
    SegmentNameGenerator segmentNameGenerator;
    FileSystemAdapter fsAdapter;
    int numCommitThreads = DEFAULT_COMMIT_THREADS;

    /**
     * Defines the basic connection parameters.
     *
     * @param pinotControllerHost Host of the Pinot controller
     * @param pinotControllerPort Port of the Pinot controller
     * @param tableName           Target table's name
     */
    public PinotSinkBuilder(String pinotControllerHost, String pinotControllerPort, String tableName) {
        this.pinotControllerHost = pinotControllerHost;
        this.pinotControllerPort = pinotControllerPort;
        this.tableName = tableName;
    }

    /**
     * Defines the serializer used to serialize elements to JSON format.
     *
     * @param jsonSerializer JsonSerializer
     * @return Builder
     */
    public PinotSinkBuilder<IN> withJsonSerializer(JsonSerializer<IN> jsonSerializer) {
        this.jsonSerializer = jsonSerializer;
        return this;
    }

    /**
     * Defines the EventTimeExtractor<IN> used to extract event times from received objects.
     *
     * @param eventTimeExtractor EventTimeExtractor
     * @return Builder
     */
    public PinotSinkBuilder<IN> withEventTimeExtractor(EventTimeExtractor<IN> eventTimeExtractor) {
        this.eventTimeExtractor = eventTimeExtractor;
        return this;
    }

    /**
     * Defines the SegmentNameGenerator used to generate names for the segments pushed to Pinot.
     *
     * @param segmentNameGenerator SegmentNameGenerator
     * @return Builder
     */
    public PinotSinkBuilder<IN> withSegmentNameGenerator(SegmentNameGenerator segmentNameGenerator) {
        this.segmentNameGenerator = segmentNameGenerator;
        return this;
    }

    /**
     * Defines a basic segment name generator which will be used to generate names for the
     * segments pushed to Pinot.
     *
     * @param segmentNamePostfix Postfix which will be appended to the segment name to identify
     *                           segments coming from this Flink sink
     * @return Builder
     */
    public PinotSinkBuilder<IN> withSimpleSegmentNameGenerator(String segmentNamePostfix) {
        return withSegmentNameGenerator(new SimpleSegmentNameGenerator(tableName, segmentNamePostfix));
    }

    /**
     * Defines the FileSystemAdapter used share data files between the {@link org.apache.flink.streaming.connectors.pinot.v2.writer.PinotWriter} and
     * the {@link PinotSinkCommitter}.
     *
     * @param fsAdapter Adapter for interacting with the shared file system
     * @return Builder
     */
    public PinotSinkBuilder<IN> withFileSystemAdapter(FileSystemAdapter fsAdapter) {
        this.fsAdapter = fsAdapter;
        return this;
    }

    /**
     * Defines the segment size via the maximum number of elements per segment.
     *
     * @param maxRowsPerSegment Maximum number of rows to be stored within a Pinot segment
     * @return Builder
     */
    public PinotSinkBuilder<IN> withMaxRowsPerSegment(int maxRowsPerSegment) {
        this.maxRowsPerSegment = maxRowsPerSegment;
        return this;
    }

    /**
     * Defines the path prefix for the files created in a node's local filesystem.
     *
     * @param tempDirPrefix Prefix for temp directories used
     * @return Builder
     */
    public PinotSinkBuilder<IN> withTempDirectoryPrefix(String tempDirPrefix) {
        this.tempDirPrefix = tempDirPrefix;
        return this;
    }

    /**
     * Defines the number of threads that shall be used to commit segments in the {@link PinotSinkCommitter}.
     *
     * @param numCommitThreads Number of threads
     * @return Builder
     */
    public PinotSinkBuilder<IN> withNumCommitThreads(int numCommitThreads) {
        this.numCommitThreads = numCommitThreads;
        return this;
    }

    /**
     * Finally builds the {@link PinotSink} according to the configuration.
     *
     * @return PinotSink
     */
    public PinotSink<IN> build() {
        return new PinotSink<>(
                pinotControllerHost,
                pinotControllerPort,
                tableName,
                maxRowsPerSegment,
                tempDirPrefix,
                jsonSerializer,
                eventTimeExtractor,
                segmentNameGenerator,
                fsAdapter,
                numCommitThreads
        );
    }
}
