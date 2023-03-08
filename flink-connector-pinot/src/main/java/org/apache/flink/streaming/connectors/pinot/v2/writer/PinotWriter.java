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

package org.apache.flink.streaming.connectors.pinot.v2.writer;

import com.google.common.collect.Iterables;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.streaming.connectors.pinot.filesystem.FileSystemAdapter;
import org.apache.flink.streaming.connectors.pinot.v2.committer.PinotSinkCommittable;
import org.apache.flink.streaming.connectors.pinot.v2.committer.PinotSinkCommitter;
import org.apache.flink.streaming.connectors.pinot.v2.external.EventTimeExtractor;
import org.apache.flink.streaming.connectors.pinot.v2.external.JsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class PinotWriter<IN> implements SinkWriter<IN>,
        StatefulSink.StatefulSinkWriter<IN, PinotWriterState>,
        TwoPhaseCommittingSink.PrecommittingSinkWriter<IN, PinotSinkCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(PinotWriter.class);

    private final int maxRowsPerSegment;
    private final EventTimeExtractor<IN> eventTimeExtractor;
    private final JsonSerializer<IN> jsonSerializer;

    private final List<PinotWriterSegment<IN>> activeSegments;
    private final FileSystemAdapter fsAdapter;

    private final int subtaskId;

    private final PinotSinkCommitter committer;


    /**
     * @param subtaskId          Subtask id provided by Flink
     * @param maxRowsPerSegment  Maximum number of rows to be stored within a Pinot segment
     * @param eventTimeExtractor Defines the way event times are extracted from received objects
     * @param jsonSerializer     Serializer used to convert elements to JSON
     * @param fsAdapter          Filesystem adapter used to save files for sharing files across nodes
     */
    public PinotWriter(int subtaskId, int maxRowsPerSegment,
                       EventTimeExtractor<IN> eventTimeExtractor,
                       JsonSerializer<IN> jsonSerializer,
                       FileSystemAdapter fsAdapter,
                       Collection<PinotWriterState> states,
                       PinotSinkCommitter committer) {
        this.subtaskId = subtaskId;
        this.maxRowsPerSegment = maxRowsPerSegment;
        this.eventTimeExtractor = checkNotNull(eventTimeExtractor);
        this.jsonSerializer = checkNotNull(jsonSerializer);
        this.fsAdapter = checkNotNull(fsAdapter);
        this.activeSegments = new ArrayList<>();
        this.committer = committer;

        if (states.size() == 1) {
            initializeState(states.iterator().next());
        } else if (states.size() > 1) {
            throw new IllegalStateException("Did not expected more than one element in states.");
        }
    }

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        final PinotWriterSegment<IN> inProgressSegment = getOrCreateInProgressSegment();
        inProgressSegment.write(element, eventTimeExtractor.getEventTime(element, context));
    }

    @Override
    public void flush(boolean flush) throws IOException, InterruptedException {
        if (flush) {
            Collection<PinotSinkCommittable> committables = prepareCommittables(flush);
            committer.commitSink(committables);
        }
    }

    @Override
    public Collection<PinotSinkCommittable> prepareCommit() throws IOException, InterruptedException {
        return prepareCommittables(false);
    }

    @Override
    public List<PinotWriterState> snapshotState(long l) throws IOException {
        final PinotWriterSegment<IN> latestSegment = Iterables.getLast(activeSegments, null);
        if (latestSegment == null || !latestSegment.acceptsElements()) {
            return new ArrayList<>();
        }

        return Collections.singletonList(latestSegment.snapshotState());
    }

    /**
     * Initializes the writer according to a previously taken snapshot.
     *
     * @param state PinotWriterState extracted from snapshot
     */
    private void initializeState(PinotWriterState state) {
        if (!activeSegments.isEmpty()) {
            throw new IllegalStateException("Please call the initialization before creating the first PinotWriterSegment.");
        }
        // Create a new PinotWriterSegment and recover its state from the given PinotSinkWriterState
        final PinotWriterSegment<IN> inProgressSegment = new PinotWriterSegment<>(maxRowsPerSegment, jsonSerializer, fsAdapter);
        inProgressSegment.initializeState(state.getSerializedElements(), state.getMinTimestamp(), state.getMaxTimestamp());
        activeSegments.add(inProgressSegment);
    }

    @Override
    public void close() throws Exception {

    }

    /**
     * Gets the {@link PinotWriterSegment} still accepting elements or creates a new one.
     *
     * @return {@link PinotWriterSegment} accepting at least one more element
     */
    private PinotWriterSegment<IN> getOrCreateInProgressSegment() {
        final PinotWriterSegment<IN> latestSegment = Iterables.getLast(activeSegments, null);
        if (latestSegment == null || !latestSegment.acceptsElements()) {
            final PinotWriterSegment<IN> inProgressSegment = new PinotWriterSegment<>(maxRowsPerSegment, jsonSerializer, fsAdapter);
            activeSegments.add(inProgressSegment);
            return inProgressSegment;
        }
        return latestSegment;
    }

    private List<PinotSinkCommittable> prepareCommittables(boolean flush) throws IOException {
        if (activeSegments.isEmpty()) return Collections.emptyList();

        // Identify segments to commit. If the flush argument is set all segments shall be committed.
        // Otherwise, take only those PinotWriterSegments that do not accept any more elements.
        List<PinotWriterSegment<IN>> segmentsToCommit = activeSegments.stream()
                .filter(s -> flush || !s.acceptsElements())
                .collect(Collectors.toList());
        LOG.debug("Identified {} segments to commit [subtaskId={}]", segmentsToCommit.size(), subtaskId);

        LOG.debug("Creating committables... [subtaskId={}]", subtaskId);
        List<PinotSinkCommittable> committables = new ArrayList<>();
        for (final PinotWriterSegment<IN> segment : segmentsToCommit) {
            committables.add(segment.prepareCommit());
        }
        LOG.debug("Created {} committables [subtaskId={}]", committables.size(), subtaskId);

        // Remove all PinotWriterSegments that will be emitted within the committables.
        activeSegments.removeAll(segmentsToCommit);
        return committables;
    }


}
