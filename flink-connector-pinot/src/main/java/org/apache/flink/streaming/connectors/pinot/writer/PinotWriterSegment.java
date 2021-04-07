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

package org.apache.flink.streaming.connectors.pinot.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.connectors.pinot.committer.PinotSinkCommittable;
import org.apache.flink.streaming.connectors.pinot.external.JsonSerializer;
import org.apache.flink.streaming.connectors.pinot.filesystem.FileSystemAdapter;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link PinotWriterSegment} represents exactly one segment that can be found in the Pinot
 * cluster once the commit has been completed.
 *
 * @param <IN> Type of incoming elements
 */
@Internal
public class PinotWriterSegment<IN> implements Serializable {

    private final int maxRowsPerSegment;
    private final JsonSerializer<IN> jsonSerializer;
    private final FileSystemAdapter fsAdapter;

    private boolean acceptsElements = true;

    private final List<String> serializedElements;
    private String dataPathOnSharedFS;
    private long minTimestamp = Long.MAX_VALUE;
    private long maxTimestamp = Long.MIN_VALUE;

    /**
     * @param maxRowsPerSegment Maximum number of rows to be stored within a Pinot segment
     * @param jsonSerializer    Serializer used to convert elements to JSON
     * @param fsAdapter         Filesystem adapter used to save files for sharing files across nodes
     */
    PinotWriterSegment(int maxRowsPerSegment, JsonSerializer<IN> jsonSerializer, FileSystemAdapter fsAdapter) {
        checkArgument(maxRowsPerSegment > 0L);
        this.maxRowsPerSegment = maxRowsPerSegment;
        this.jsonSerializer = checkNotNull(jsonSerializer);
        this.fsAdapter = checkNotNull(fsAdapter);
        this.serializedElements = new ArrayList<>();
    }

    /**
     * Takes elements and stores them in memory until either {@link #maxRowsPerSegment} is reached
     * or {@link #prepareCommit} is called.
     *
     * @param element   Object from upstream task
     * @param timestamp Timestamp assigned to element
     * @throws IOException
     */
    public void write(IN element, long timestamp) throws IOException {
        if (!acceptsElements()) {
            throw new IllegalStateException("This PinotSegmentWriter does not accept any elements anymore.");
        }
        // Store serialized element in serializedElements
        serializedElements.add(jsonSerializer.toJson(element));
        minTimestamp = Long.min(minTimestamp, timestamp);
        maxTimestamp = Long.max(maxTimestamp, timestamp);

        // Writes elements to local filesystem once the maximum number of items is reached
        if (serializedElements.size() == maxRowsPerSegment) {
            acceptsElements = false;
            dataPathOnSharedFS = writeToSharedFilesystem();
            serializedElements.clear();
        }
    }

    /**
     * Writes elements to local file (if not already done). Copies just created file to the shared
     * filesystem defined via {@link FileSystemAdapter} and creates a {@link PinotSinkCommittable}.
     *
     * @return {@link PinotSinkCommittable} pointing to file on shared filesystem
     * @throws IOException
     */
    public PinotSinkCommittable prepareCommit() throws IOException {
        if (dataPathOnSharedFS == null) {
            dataPathOnSharedFS = writeToSharedFilesystem();
        }
        return new PinotSinkCommittable(dataPathOnSharedFS, minTimestamp, maxTimestamp);
    }

    /**
     * Takes elements from {@link #serializedElements} and writes them to the shared filesystem.
     *
     * @return Path pointing to just written data on shared filesystem
     * @throws IOException
     */
    private String writeToSharedFilesystem() throws IOException {
        return fsAdapter.writeToSharedFileSystem(serializedElements);
    }

    /**
     * Determines whether this segment can accept at least one more elements
     *
     * @return True if at least one more element will be accepted
     */
    public boolean acceptsElements() {
        return acceptsElements;
    }

    /**
     * Recovers a previously written state.
     *
     * @param _serializedElements List containing received, but not yet committed list of serialized elements.
     * @param _minTimestamp       Minimum event timestamp of all elements
     * @param _maxTimestamp       Maximum event timestamp of all elements
     */
    public void initializeState(List<String> _serializedElements, long _minTimestamp, long _maxTimestamp) {
        if (!serializedElements.isEmpty()) {
            throw new IllegalStateException("Cannot initialize a PinotWriterSegment that has already received elements.");
        }

        serializedElements.addAll(_serializedElements);
        minTimestamp = _minTimestamp;
        maxTimestamp = _maxTimestamp;
    }

    /**
     * Snapshots the current state of an active {@link PinotWriterSegment}.
     *
     * @return List of elements currently stored within the {@link PinotWriterSegment}
     */
    public PinotSinkWriterState snapshotState() {
        if (!acceptsElements()) {
            throw new IllegalStateException("Snapshots can only be created of in-progress segments.");
        }

        return new PinotSinkWriterState(serializedElements, minTimestamp, maxTimestamp);
    }
}
