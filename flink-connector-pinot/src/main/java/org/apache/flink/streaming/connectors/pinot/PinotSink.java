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

package org.apache.flink.streaming.connectors.pinot;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.pinot.committer.PinotSinkCommittable;
import org.apache.flink.streaming.connectors.pinot.committer.PinotSinkGlobalCommittable;
import org.apache.flink.streaming.connectors.pinot.committer.PinotSinkGlobalCommitter;
import org.apache.flink.streaming.connectors.pinot.external.EventTimeExtractor;
import org.apache.flink.streaming.connectors.pinot.external.JsonSerializer;
import org.apache.flink.streaming.connectors.pinot.filesystem.FileSystemAdapter;
import org.apache.flink.streaming.connectors.pinot.segment.name.PinotSinkSegmentNameGenerator;
import org.apache.flink.streaming.connectors.pinot.segment.name.SimpleSegmentNameGenerator;
import org.apache.flink.streaming.connectors.pinot.serializer.PinotSinkCommittableSerializer;
import org.apache.flink.streaming.connectors.pinot.serializer.PinotSinkGlobalCommittableSerializer;
import org.apache.flink.streaming.connectors.pinot.serializer.PinotSinkWriterStateSerializer;
import org.apache.flink.streaming.connectors.pinot.writer.PinotSinkWriter;
import org.apache.flink.streaming.connectors.pinot.writer.PinotSinkWriterState;
import org.apache.flink.streaming.connectors.pinot.writer.PinotWriterSegment;
import org.apache.pinot.core.segment.creator.SegmentIndexCreationDriver;
import org.apache.pinot.core.segment.name.SegmentNameGenerator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.tools.admin.command.UploadSegmentCommand;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Apache Pinot sink that stores objects from upstream Flink tasks in a Apache Pinot table. The sink
 * can be operated in {@code RuntimeExecutionMode.STREAMING} or {@code RuntimeExecutionMode.BATCH}
 * mode. But ensure to enable checkpointing when using in streaming mode.
 *
 * <p>We advise you to use the provided {@link PinotSink.Builder} to build and configure the
 * PinotSink. All the communication with the Pinot cluster's table is managed via the Pinot
 * controller. Thus you need to provide its host and port as well as the target Pinot table.
 * The {@link TableConfig} and {@link Schema} is automatically retrieved via the Pinot controller API
 * and therefore does not need to be provided.
 *
 * <p>Whenever an element is received by the sink it gets stored in a {@link PinotWriterSegment}. A
 * {@link PinotWriterSegment} represents exactly one segment that will be pushed to the Pinot
 * cluster later on. Its size is determined by the customizable {@code maxRowsPerSegment} parameter.
 * Please note that the maximum segment size that can be handled by this sink is limited by the
 * lower bound of memory available at each subTask.
 * Each subTask holds a list of {@link PinotWriterSegment}s of which at most one is active. An
 * active {@link PinotWriterSegment} is capable of accepting at least one more element. If a
 * {@link PinotWriterSegment} switches from active to inactive it flushes its
 * {@code maxRowsPerSegment} elements to disk. The data file is stored in the local filesystem's
 * temporary directory and contains serialized elements. We use the {@link JsonSerializer} to
 * serialize elements to JSON.
 *
 * <p>On checkpointing all not in-progress {@link PinotWriterSegment}s are transformed into
 * committables. As the data files need to be shared across nodes, the sink requires access to a
 * shared filesystem. We use the {@link FileSystemAdapter} for that purpose.
 * A {@link FileSystemAdapter} is capable of copying a file from the local to the shared filesystem
 * and vice-versa. A {@link PinotSinkCommittable} contains a reference to a data file on the shared
 * filesystem as well as the minimum and maximum timestamp contained in the data file. A timestamp -
 * usually the event time - is extracted from each received element via {@link EventTimeExtractor}.
 * The timestamps are later on required to follow the guideline for naming Pinot segments.
 * An eventually existent in-progress {@link PinotWriterSegment}'s state is saved in the snapshot
 * taken when checkpointing. This ensures that the at-most-once delivery guarantee can be fulfilled
 * when recovering from failures.
 *
 * <p>We use the {@link PinotSinkGlobalCommitter} to collect all created
 * {@link PinotSinkCommittable}s, create segments from the referenced data files and finally push them
 * to the Pinot table. Therefore, the minimum and maximum timestamp of all
 * {@link PinotSinkCommittable} is determined. The segment names are then generated using the
 * {@link PinotSinkSegmentNameGenerator} which gets the minimum and maximum timestamp as input.
 * The segment generation starts with downloading the referenced data file from the shared file system
 * using the provided {@link FileSystemAdapter}. Once this is was completed, we use Pinot's
 * {@link SegmentIndexCreationDriver} to generate the final segment. Each segment is thereby stored
 * in a temporary directory on the local filesystem. Next, the segment is uploaded to the Pinot
 * controller using Pinot's {@link UploadSegmentCommand}.
 *
 * <p>To ensure that possible failures are handled accordingly each segment name is checked for
 * existence within the Pinot cluster before uploading a segment. In case a segment name already
 * exists, i.e. if the last commit failed partially with some segments already been uploaded, the
 * existing segment is deleted first. When the elements since the last checkpoint are replayed the
 * minimum and maximum timestamp of all received elements will be the same. Thus the same set of
 * segment names is generated and we can delete previous segments by checking for segment name
 * presence. Note: The {@link PinotSinkSegmentNameGenerator} must be deterministic. We also provide
 * a {@link SimpleSegmentNameGenerator} which is a simple but for most users suitable segment name
 * generator.
 *
 * <p>Please note that we use the {@link GlobalCommitter} to ensure consistent segment naming. This
 * comes with performance limitations as a {@link GlobalCommitter} always runs at a parallelism of 1
 * which results in a clear bottleneck at the {@link PinotSinkGlobalCommitter} that does all the
 * computational intensive work (i.e. generating and uploading segments). In order to overcome this
 * issue we introduce a custom multithreading approach within the {@link PinotSinkGlobalCommitter}
 * to parallelize the segment creation and upload process.
 *
 * @param <IN> Type of incoming elements
 */
public class PinotSink<IN> implements Sink<IN, PinotSinkCommittable, PinotSinkWriterState, PinotSinkGlobalCommittable> {

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
     * @param numCommitThreads     Number of threads used in the {@link PinotSinkGlobalCommitter} for committing segments
     */
    private PinotSink(String pinotControllerHost, String pinotControllerPort, String tableName,
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

    /**
     * Creates a Pinot sink writer.
     *
     * @param context InitContext
     * @param states  State extracted from snapshot. This list must not have a size larger than 1
     */
    @Override
    public PinotSinkWriter<IN> createWriter(InitContext context, List<PinotSinkWriterState> states) {
        PinotSinkWriter<IN> writer = new PinotSinkWriter<>(
                context.getSubtaskId(), maxRowsPerSegment, eventTimeExtractor,
                jsonSerializer, fsAdapter
        );

        if (states.size() == 1) {
            writer.initializeState(states.get(0));
        } else if (states.size() > 1) {
            throw new IllegalStateException("Did not expected more than one element in states.");
        }
        return writer;
    }

    /**
     * The PinotSink does not use a committer. Instead a global committer is used
     *
     * @return Empty Optional
     */
    @Override
    public Optional<Committer<PinotSinkCommittable>> createCommitter() {
        return Optional.empty();
    }

    /**
     * Creates the global committer.
     */
    @Override
    public Optional<GlobalCommitter<PinotSinkCommittable, PinotSinkGlobalCommittable>> createGlobalCommitter() throws IOException {
        String timeColumnName = eventTimeExtractor.getTimeColumn();
        TimeUnit segmentTimeUnit = eventTimeExtractor.getSegmentTimeUnit();
        PinotSinkGlobalCommitter committer = new PinotSinkGlobalCommitter(
                pinotControllerHost, pinotControllerPort, tableName, segmentNameGenerator,
                tempDirPrefix, fsAdapter, timeColumnName, segmentTimeUnit, numCommitThreads
        );
        return Optional.of(committer);
    }

    /**
     * Creates the committables' serializer.
     */
    @Override
    public Optional<SimpleVersionedSerializer<PinotSinkCommittable>> getCommittableSerializer() {
        return Optional.of(new PinotSinkCommittableSerializer());
    }

    /**
     * Creates the global committables' serializer.
     */
    @Override
    public Optional<SimpleVersionedSerializer<PinotSinkGlobalCommittable>> getGlobalCommittableSerializer() {
        return Optional.of(new PinotSinkGlobalCommittableSerializer());
    }

    /**
     * The PinotSink does not use writer states.
     *
     * @return Empty Optional
     */
    @Override
    public Optional<SimpleVersionedSerializer<PinotSinkWriterState>> getWriterStateSerializer() {
        return Optional.of(new PinotSinkWriterStateSerializer());
    }

    /**
     * Builder for configuring a {@link PinotSink}. This is the recommended public API.
     *
     * @param <IN> Type of incoming elements
     */
    public static class Builder<IN> {

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
        public Builder(String pinotControllerHost, String pinotControllerPort, String tableName) {
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
        public Builder<IN> withJsonSerializer(JsonSerializer<IN> jsonSerializer) {
            this.jsonSerializer = jsonSerializer;
            return this;
        }

        /**
         * Defines the EventTimeExtractor<IN> used to extract event times from received objects.
         *
         * @param eventTimeExtractor EventTimeExtractor
         * @return Builder
         */
        public Builder<IN> withEventTimeExtractor(EventTimeExtractor<IN> eventTimeExtractor) {
            this.eventTimeExtractor = eventTimeExtractor;
            return this;
        }

        /**
         * Defines the SegmentNameGenerator used to generate names for the segments pushed to Pinot.
         *
         * @param segmentNameGenerator SegmentNameGenerator
         * @return Builder
         */
        public Builder<IN> withSegmentNameGenerator(SegmentNameGenerator segmentNameGenerator) {
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
        public Builder<IN> withSimpleSegmentNameGenerator(String segmentNamePostfix) {
            return withSegmentNameGenerator(new SimpleSegmentNameGenerator(tableName, segmentNamePostfix));
        }

        /**
         * Defines the FileSystemAdapter used share data files between the {@link PinotSinkWriter} and
         * the {@link PinotSinkGlobalCommitter}.
         *
         * @param fsAdapter Adapter for interacting with the shared file system
         * @return Builder
         */
        public Builder<IN> withFileSystemAdapter(FileSystemAdapter fsAdapter) {
            this.fsAdapter = fsAdapter;
            return this;
        }

        /**
         * Defines the segment size via the maximum number of elements per segment.
         *
         * @param maxRowsPerSegment Maximum number of rows to be stored within a Pinot segment
         * @return Builder
         */
        public Builder<IN> withMaxRowsPerSegment(int maxRowsPerSegment) {
            this.maxRowsPerSegment = maxRowsPerSegment;
            return this;
        }

        /**
         * Defines the path prefix for the files created in a node's local filesystem.
         *
         * @param tempDirPrefix Prefix for temp directories used
         * @return Builder
         */
        public Builder<IN> withTempDirectoryPrefix(String tempDirPrefix) {
            this.tempDirPrefix = tempDirPrefix;
            return this;
        }

        /**
         * Defines the number of threads that shall be used to commit segments in the {@link PinotSinkGlobalCommitter}.
         *
         * @param numCommitThreads Number of threads
         * @return Builder
         */
        public Builder<IN> withNumCommitThreads(int numCommitThreads) {
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
}
