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

package org.apache.flink.streaming.connectors.pinot.writer;

import com.google.common.collect.Iterables;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.streaming.connectors.pinot.committer.PinotSinkCommittable;
import org.apache.flink.streaming.connectors.pinot.external.EventTimeExtractor;
import org.apache.flink.streaming.connectors.pinot.external.JsonSerializer;
import org.apache.flink.streaming.connectors.pinot.filesystem.FileSystemAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Accepts incoming elements and creates {@link PinotSinkCommittable}s out of them on request.
 *
 * @param <IN> Type of incoming elements
 */
@Internal
public class PinotSinkWriter<IN> implements SinkWriter<IN, PinotSinkCommittable, PinotSinkWriterState> {

    private static final Logger LOG = LoggerFactory.getLogger(PinotSinkWriter.class);

    private final int maxRowsPerSegment;
    private final EventTimeExtractor<IN> eventTimeExtractor;
    private final JsonSerializer<IN> jsonSerializer;

    private final List<PinotWriterSegment<IN>> activeSegments;
    private final FileSystemAdapter fsAdapter;

    private final int subtaskId;

    /**
     * @param subtaskId          Subtask id provided by Flink
     * @param maxRowsPerSegment  Maximum number of rows to be stored within a Pinot segment
     * @param eventTimeExtractor Defines the way event times are extracted from received objects
     * @param jsonSerializer     Serializer used to convert elements to JSON
     * @param fsAdapter          Filesystem adapter used to save files for sharing files across nodes
     */
    public PinotSinkWriter(int subtaskId, int maxRowsPerSegment,
                           EventTimeExtractor<IN> eventTimeExtractor,
                           JsonSerializer<IN> jsonSerializer, FileSystemAdapter fsAdapter) {
        this.subtaskId = subtaskId;
        this.maxRowsPerSegment = maxRowsPerSegment;
        this.eventTimeExtractor = checkNotNull(eventTimeExtractor);
        this.jsonSerializer = checkNotNull(jsonSerializer);
        this.fsAdapter = checkNotNull(fsAdapter);
        this.activeSegments = new ArrayList<>();
    }

    /**
     * Takes elements from an upstream tasks and writes them into {@link PinotWriterSegment}
     *
     * @param element Object from upstream task
     * @param context SinkWriter context
     * @throws IOException
     */
    @Override
    public void write(IN element, Context context) throws IOException {
        final PinotWriterSegment<IN> inProgressSegment = getOrCreateInProgressSegment();
        inProgressSegment.write(element, eventTimeExtractor.getEventTime(element, context));
    }

    /**
     * Creates {@link PinotSinkCommittable}s from elements previously received via {@link #write}.
     * If flush is set, all {@link PinotWriterSegment}s are transformed into
     * {@link PinotSinkCommittable}s. If flush is not set, only currently non-active
     * {@link PinotSinkCommittable}s are transformed into {@link PinotSinkCommittable}s.
     * To convert a {@link PinotWriterSegment} into a {@link PinotSinkCommittable} the data gets
     * written to the shared filesystem. Moreover, minimum and maximum timestamps are identified.
     * Finally, all {@link PinotWriterSegment}s transformed into {@link PinotSinkCommittable}s are
     * removed from {@link #activeSegments}.
     *
     * @param flush Flush all currently known elements into the {@link PinotSinkCommittable}s
     * @return List of {@link PinotSinkCommittable} to process in {@link org.apache.flink.streaming.connectors.pinot.committer.PinotSinkGlobalCommitter}
     * @throws IOException
     */
    @Override
    public List<PinotSinkCommittable> prepareCommit(boolean flush) throws IOException {
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

    /**
     * Snapshots the latest PinotWriterSegment (if existent), so that the contained (and not yet
     * committed) elements can be recovered later on in case of a failure.
     *
     * @return A list containing at most one PinotSinkWriterState
     */
    @Override
    public List<PinotSinkWriterState> snapshotState() {
        final PinotWriterSegment<IN> latestSegment = Iterables.getLast(activeSegments, null);
        if (latestSegment == null || !latestSegment.acceptsElements()) {
            return new ArrayList<>();
        }

        return Collections.singletonList(latestSegment.snapshotState());
    }

    /**
     * Initializes the writer according to a previously taken snapshot.
     *
     * @param state PinotSinkWriterState extracted from snapshot
     */
    public void initializeState(PinotSinkWriterState state) {
        if (activeSegments.size() != 0) {
            throw new IllegalStateException("Please call the initialization before creating the first PinotWriterSegment.");
        }
        // Create a new PinotWriterSegment and recover its state from the given PinotSinkWriterState
        final PinotWriterSegment<IN> inProgressSegment = new PinotWriterSegment<>(maxRowsPerSegment, jsonSerializer, fsAdapter);
        inProgressSegment.initializeState(state.getSerializedElements(), state.getMinTimestamp(), state.getMaxTimestamp());
        activeSegments.add(inProgressSegment);
    }

    /**
     * Empty method, as we do not open any connections.
     */
    @Override
    public void close() {
    }
}
