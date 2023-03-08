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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.pinot.filesystem.FileSystemAdapter;
import org.apache.flink.streaming.connectors.pinot.v2.committer.PinotSinkCommittable;
import org.apache.flink.streaming.connectors.pinot.v2.committer.PinotSinkCommittableSerializer;
import org.apache.flink.streaming.connectors.pinot.v2.committer.PinotSinkCommitter;
import org.apache.flink.streaming.connectors.pinot.v2.external.EventTimeExtractor;
import org.apache.flink.streaming.connectors.pinot.v2.external.JsonSerializer;
import org.apache.flink.streaming.connectors.pinot.v2.writer.PinotWriter;
import org.apache.flink.streaming.connectors.pinot.v2.writer.PinotWriterState;
import org.apache.flink.streaming.connectors.pinot.v2.writer.PinotWriterStateSerializer;
import org.apache.pinot.core.segment.name.SegmentNameGenerator;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class PinotSink<IN> implements
        Sink<IN>,
        StatefulSink<IN, PinotWriterState>,
        TwoPhaseCommittingSink<IN, PinotSinkCommittable> {

    private final String pinotControllerHost;
    private final String pinotControllerPort;
    private final String tableName;
    private final int maxRowsPerSegment;
    private final String tempDirPrefix;
    private final JsonSerializer<IN> jsonSerializer;
    private final SegmentNameGenerator segmentNameGenerator;
    private final FileSystemAdapter fsAdapter;
    private final EventTimeExtractor<IN> eventTimeExtractor;
    private final int numCommitThreads;

    /**
     * @param pinotControllerHost  Host of the Pinot controller
     * @param pinotControllerPort  Port of the Pinot controller
     * @param tableName            Target table's name
     * @param maxRowsPerSegment    Maximum number of rows to be stored within a Pinot segment
     * @param tempDirPrefix        Prefix for temp directories used
     * @param jsonSerializer       Serializer used to convert elements to JSON
     * @param eventTimeExtractor   Defines the way event times are extracted from received objects
     * @param segmentNameGenerator Pinot segment name generator
     * @param fsAdapter            Filesystem adapter used to save files for sharing files across nodes
     * @param numCommitThreads     Number of threads used in the {@link PinotSinkCommitter} for committing segments
     */
    protected PinotSink(String pinotControllerHost, String pinotControllerPort, String tableName,
                      int maxRowsPerSegment, String tempDirPrefix, JsonSerializer<IN> jsonSerializer,
                      EventTimeExtractor<IN> eventTimeExtractor,
                      SegmentNameGenerator segmentNameGenerator, FileSystemAdapter fsAdapter,
                      int numCommitThreads) {
        this.pinotControllerHost = checkNotNull(pinotControllerHost);
        this.pinotControllerPort = checkNotNull(pinotControllerPort);
        this.tableName = checkNotNull(tableName);

        checkArgument(maxRowsPerSegment > 0);
        this.maxRowsPerSegment = maxRowsPerSegment;
        this.tempDirPrefix = checkNotNull(tempDirPrefix);
        this.jsonSerializer = checkNotNull(jsonSerializer);
        this.eventTimeExtractor = checkNotNull(eventTimeExtractor);
        this.segmentNameGenerator = checkNotNull(segmentNameGenerator);
        this.fsAdapter = checkNotNull(fsAdapter);
        checkArgument(numCommitThreads > 0);
        this.numCommitThreads = numCommitThreads;
    }


    @Override
    public PinotWriter<IN> createWriter(InitContext context) throws IOException {
        return this.restoreWriter(context, Collections.emptyList());
    }

    @Override
    public PinotWriter<IN> restoreWriter(InitContext context, Collection<PinotWriterState> states) throws IOException {
        PinotWriter<IN> writer = new PinotWriter<>(
                context.getSubtaskId(), maxRowsPerSegment, eventTimeExtractor,
                jsonSerializer, fsAdapter, states, this.createCommitter()
        );
        return writer;
    }

    @Override
    public PinotSinkCommitter createCommitter() throws IOException {
        String timeColumnName = eventTimeExtractor.getTimeColumn();
        TimeUnit segmentTimeUnit = eventTimeExtractor.getSegmentTimeUnit();
        PinotSinkCommitter committer = new PinotSinkCommitter(
                pinotControllerHost, pinotControllerPort, tableName, segmentNameGenerator,
                tempDirPrefix, fsAdapter, timeColumnName, segmentTimeUnit, numCommitThreads
        );
        return committer;
    }

    @Override
    public SimpleVersionedSerializer<PinotSinkCommittable> getCommittableSerializer() {
        return new PinotSinkCommittableSerializer();
    }

    @Override
    public SimpleVersionedSerializer<PinotWriterState> getWriterStateSerializer() {
        return new PinotWriterStateSerializer();
    }

}
