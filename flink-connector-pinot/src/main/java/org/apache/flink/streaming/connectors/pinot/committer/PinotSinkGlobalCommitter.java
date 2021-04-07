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

package org.apache.flink.streaming.connectors.pinot.committer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.streaming.connectors.pinot.PinotControllerClient;
import org.apache.flink.streaming.connectors.pinot.filesystem.FileSystemAdapter;
import org.apache.flink.streaming.connectors.pinot.filesystem.FileSystemUtils;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.segment.creator.SegmentIndexCreationDriver;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.core.segment.name.SegmentNameGenerator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.tools.admin.command.UploadSegmentCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Global committer takes committables from {@link org.apache.flink.streaming.connectors.pinot.writer.PinotSinkWriter},
 * generates segments and pushed them to the Pinot controller.
 * Note: We use a custom multithreading approach to parallelize the segment creation and upload to
 * overcome the performance limitations resulting from using a {@link GlobalCommitter} always
 * running at a parallelism of 1.
 */
@Internal
public class PinotSinkGlobalCommitter implements GlobalCommitter<PinotSinkCommittable, PinotSinkGlobalCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(PinotSinkGlobalCommitter.class);

    private final String pinotControllerHost;
    private final String pinotControllerPort;
    private final String tableName;
    private final SegmentNameGenerator segmentNameGenerator;
    private final FileSystemAdapter fsAdapter;
    private final String timeColumnName;
    private final TimeUnit segmentTimeUnit;
    private final PinotControllerClient pinotControllerClient;
    private final File tempDirectory;
    private final Schema tableSchema;
    private final TableConfig tableConfig;
    private final ExecutorService pool;

    /**
     * @param pinotControllerHost  Host of the Pinot controller
     * @param pinotControllerPort  Port of the Pinot controller
     * @param tableName            Target table's name
     * @param segmentNameGenerator Pinot segment name generator
     * @param tempDirPrefix        Prefix for directory to store temporary files in
     * @param fsAdapter            Adapter for interacting with the shared file system
     * @param timeColumnName       Name of the column containing the timestamp
     * @param segmentTimeUnit      Unit of the time column
     * @param numCommitThreads     Number of threads used to commit the committables
     */
    public PinotSinkGlobalCommitter(String pinotControllerHost, String pinotControllerPort,
                                    String tableName, SegmentNameGenerator segmentNameGenerator,
                                    String tempDirPrefix, FileSystemAdapter fsAdapter,
                                    String timeColumnName, TimeUnit segmentTimeUnit,
                                    int numCommitThreads) throws IOException {
        this.pinotControllerHost = checkNotNull(pinotControllerHost);
        this.pinotControllerPort = checkNotNull(pinotControllerPort);
        this.tableName = checkNotNull(tableName);
        this.segmentNameGenerator = checkNotNull(segmentNameGenerator);
        this.fsAdapter = checkNotNull(fsAdapter);
        this.timeColumnName = checkNotNull(timeColumnName);
        this.segmentTimeUnit = checkNotNull(segmentTimeUnit);
        this.pinotControllerClient = new PinotControllerClient(pinotControllerHost, pinotControllerPort);

        // Create directory that temporary files will be stored in
        this.tempDirectory = Files.createTempDirectory(tempDirPrefix).toFile();

        // Retrieve the Pinot table schema and the Pinot table config from the Pinot controller
        this.tableSchema = pinotControllerClient.getSchema(tableName);
        this.tableConfig = pinotControllerClient.getTableConfig(tableName);

        // We use a thread pool in order to parallelize the segment creation and segment upload
        checkArgument(numCommitThreads > 0);
        this.pool = Executors.newFixedThreadPool(numCommitThreads);
    }

    /**
     * Identifies global committables that need to be re-committed from a list of recovered committables.
     *
     * @param globalCommittables List of global committables that are checked for required re-commit
     * @return List of global committable that need to be re-committed
     * @throws IOException
     */
    @Override
    public List<PinotSinkGlobalCommittable> filterRecoveredCommittables(List<PinotSinkGlobalCommittable> globalCommittables) throws IOException {
        // Holds identified global committables whose commit needs to be retried
        List<PinotSinkGlobalCommittable> committablesToRetry = new ArrayList<>();

        for (PinotSinkGlobalCommittable globalCommittable : globalCommittables) {
            CommitStatus commitStatus = getCommitStatus(globalCommittable);

            if (commitStatus.getMissingSegmentNames().isEmpty()) {
                // All segments were already committed. Thus, we do not need to retry the commit.
                continue;
            }

            for (String existingSegment : commitStatus.getExistingSegmentNames()) {
                // Some but not all segments were already committed. As we cannot assure the data
                // files containing the same data as originally when recovering from failure,
                // we delete the already committed segments in order to recommit them later on.
                pinotControllerClient.deleteSegment(tableName, existingSegment);
            }
            committablesToRetry.add(globalCommittable);
        }
        return committablesToRetry;
    }

    /**
     * Combines multiple {@link PinotSinkCommittable}s into one {@link PinotSinkGlobalCommittable}
     * by finding the minimum and maximum timestamps from the provided {@link PinotSinkCommittable}s.
     *
     * @param committables Committables created by {@link org.apache.flink.streaming.connectors.pinot.writer.PinotSinkWriter}
     * @return Global committer committable
     */
    @Override
    public PinotSinkGlobalCommittable combine(List<PinotSinkCommittable> committables) {
        List<String> dataFilePaths = new ArrayList<>();
        long minTimestamp = Long.MAX_VALUE;
        long maxTimestamp = Long.MIN_VALUE;

        // Extract all data file paths and the overall minimum and maximum timestamps
        // from all committables
        for (PinotSinkCommittable committable : committables) {
            dataFilePaths.add(committable.getDataFilePath());
            minTimestamp = Long.min(minTimestamp, committable.getMinTimestamp());
            maxTimestamp = Long.max(maxTimestamp, committable.getMaxTimestamp());
        }

        LOG.debug("Combined {} committables into one global committable", committables.size());
        return new PinotSinkGlobalCommittable(dataFilePaths, minTimestamp, maxTimestamp);
    }

    /**
     * Copies data files from shared filesystem to the local filesystem, generates segments with names
     * according to the segment naming schema and finally pushes the segments to the Pinot cluster.
     * Before pushing a segment it is checked whether there already exists a segment with that name
     * in the Pinot cluster by calling the Pinot controller. In case there is one, it gets deleted.
     *
     * @param globalCommittables List of global committables
     * @return Global committables whose commit failed
     * @throws IOException
     */
    @Override
    public List<PinotSinkGlobalCommittable> commit(List<PinotSinkGlobalCommittable> globalCommittables) throws IOException {
        // List of failed global committables that can be retried later on
        List<PinotSinkGlobalCommittable> failedCommits = new ArrayList<>();

        for (PinotSinkGlobalCommittable globalCommittable : globalCommittables) {
            Set<Future<Boolean>> resultFutures = new HashSet<>();
            // Commit all segments in globalCommittable
            for (int sequenceId = 0; sequenceId < globalCommittable.getDataFilePaths().size(); sequenceId++) {
                String dataFilePath = globalCommittable.getDataFilePaths().get(sequenceId);
                // Get segment names with increasing sequenceIds
                String segmentName = getSegmentName(globalCommittable, sequenceId);
                // Segment committer handling the whole commit process for a single segment
                Callable<Boolean> segmentCommitter = new SegmentCommitter(
                        pinotControllerHost, pinotControllerPort, tempDirectory, fsAdapter,
                        dataFilePath, segmentName, tableSchema, tableConfig, timeColumnName,
                        segmentTimeUnit
                );
                // Submits the segment committer to the thread pool
                resultFutures.add(pool.submit(segmentCommitter));
            }

            boolean commitSucceeded = true;
            try {
                for (Future<Boolean> wasSuccessful : resultFutures) {
                    // In case any of the segment commits wasn't successful we mark the whole
                    // globalCommittable as failed
                    if (!wasSuccessful.get()) {
                        commitSucceeded = false;
                        failedCommits.add(globalCommittable);
                        // Once any of the commits failed, we do not need to check the remaining
                        // ones, as we try to commit the globalCommittable next time
                        break;
                    }
                }
            } catch (InterruptedException | ExecutionException e) {
                // In case of an exception thrown while accessing commit status, mark the whole
                // globalCommittable as failed
                failedCommits.add(globalCommittable);
                LOG.error("Accessing a SegmentCommitter thread errored with {}", e.getMessage(), e);
            }

            if (commitSucceeded) {
                // If commit succeeded, cleanup the data files stored on the shared file system. In
                // case the commit of at least one of the segments failed, nothing will be cleaned
                // up here to enable retrying failed commits (data files must therefore stay
                // available on the shared filesystem).
                for (String path : globalCommittable.getDataFilePaths()) {
                    fsAdapter.deleteFromSharedFileSystem(path);
                }
            }
        }

        // Return failed commits so that they can be retried later on
        return failedCommits;
    }

    /**
     * Empty method.
     */
    @Override
    public void endOfInput() {
    }

    /**
     * Closes the Pinot controller http client, clears the created temporary directory and
     * shuts the thread pool down.
     */
    @Override
    public void close() throws IOException {
        pinotControllerClient.close();
        tempDirectory.delete();
        pool.shutdown();
    }

    /**
     * Helper method for generating segment names using the segment name generator.
     *
     * @param globalCommittable Global committable the segment name shall be generated from
     * @param sequenceId        Incrementing counter
     * @return generated segment name
     */
    private String getSegmentName(PinotSinkGlobalCommittable globalCommittable, int sequenceId) {
        return segmentNameGenerator.generateSegmentName(sequenceId,
                globalCommittable.getMinTimestamp(), globalCommittable.getMaxTimestamp());
    }

    /**
     * Evaluates the status of already uploaded segments by requesting segment metadata from the
     * Pinot controller.
     *
     * @param globalCommittable Global committable whose commit status gets evaluated
     * @return Commit status
     * @throws IOException
     */
    private CommitStatus getCommitStatus(PinotSinkGlobalCommittable globalCommittable) throws IOException {
        List<String> existingSegmentNames = new ArrayList<>();
        List<String> missingSegmentNames = new ArrayList<>();

        // For all segment names that will be used to submit new segments, check whether the segment
        // name already exists for the target table
        for (int sequenceId = 0; sequenceId < globalCommittable.getDataFilePaths().size(); sequenceId++) {
            String segmentName = getSegmentName(globalCommittable, sequenceId);
            if (pinotControllerClient.tableHasSegment(tableName, segmentName)) {
                // Segment name already exists
                existingSegmentNames.add(segmentName);
            } else {
                // Segment name does not exist yet
                missingSegmentNames.add(segmentName);
            }
        }
        return new CommitStatus(existingSegmentNames, missingSegmentNames);
    }

    /**
     * Wrapper for existing and missing segments in the Pinot cluster.
     */
    private static class CommitStatus {
        private final List<String> existingSegmentNames;
        private final List<String> missingSegmentNames;

        CommitStatus(List<String> existingSegmentNames, List<String> missingSegmentNames) {
            this.existingSegmentNames = existingSegmentNames;
            this.missingSegmentNames = missingSegmentNames;
        }

        public List<String> getExistingSegmentNames() {
            return existingSegmentNames;
        }

        public List<String> getMissingSegmentNames() {
            return missingSegmentNames;
        }
    }

    /**
     * Helper class for committing a single segment. Downloads a data file from the shared filesystem,
     * generates a segment from the data file and uploads segment to the Pinot controller.
     */
    private static class SegmentCommitter implements Callable<Boolean> {

        private static final Logger LOG = LoggerFactory.getLogger(SegmentCommitter.class);

        private final String pinotControllerHost;
        private final String pinotControllerPort;
        private final File tempDirectory;
        private final FileSystemAdapter fsAdapter;
        private final String dataFilePath;
        private final String segmentName;
        private final Schema tableSchema;
        private final TableConfig tableConfig;
        private final String timeColumnName;
        private final TimeUnit segmentTimeUnit;

        /**
         * @param pinotControllerHost Host of the Pinot controller
         * @param pinotControllerPort Port of the Pinot controller
         * @param tempDirectory       Directory to store temporary files in
         * @param fsAdapter           Filesystem adapter used to load data files from the shared file system
         * @param dataFilePath        Data file to load from the shared file system
         * @param segmentName         Name of the segment to create and commit
         * @param tableSchema         Pinot table schema
         * @param tableConfig         Pinot table config
         * @param timeColumnName      Name of the column containing the timestamp
         * @param segmentTimeUnit     Unit of the time column
         */
        SegmentCommitter(String pinotControllerHost, String pinotControllerPort,
                         File tempDirectory, FileSystemAdapter fsAdapter,
                         String dataFilePath, String segmentName, Schema tableSchema,
                         TableConfig tableConfig, String timeColumnName,
                         TimeUnit segmentTimeUnit) {
            this.pinotControllerHost = pinotControllerHost;
            this.pinotControllerPort = pinotControllerPort;
            this.tempDirectory = tempDirectory;
            this.fsAdapter = fsAdapter;
            this.dataFilePath = dataFilePath;
            this.segmentName = segmentName;
            this.tableSchema = tableSchema;
            this.tableConfig = tableConfig;
            this.timeColumnName = timeColumnName;
            this.segmentTimeUnit = segmentTimeUnit;
        }

        /**
         * Downloads a segment from the shared file system via {@code fsAdapter}, generates a segment
         * and finally uploads the segment to the Pinot controller
         *
         * @return True if the commit succeeded
         */
        @Override
        public Boolean call() {
            // Local copy of data file stored on the shared filesystem
            File segmentData = null;
            // File containing the final Pinot segment
            File segmentFile = null;
            try {
                // Download data file from the shared filesystem
                LOG.debug("Downloading data file {} from shared file system...", dataFilePath);
                List<String> serializedElements = fsAdapter.readFromSharedFileSystem(dataFilePath);
                segmentData = FileSystemUtils.writeToLocalFile(serializedElements, tempDirectory);
                LOG.debug("Successfully downloaded data file {} from shared file system", dataFilePath);

                segmentFile = FileSystemUtils.createFileInDir(tempDirectory);
                LOG.debug("Creating segment in " + segmentFile.getAbsolutePath());

                // Creates a segment with name `segmentName` in `segmentFile`
                generateSegment(segmentData, segmentFile, true);

                // Uploads the recently created segment to the Pinot controller
                uploadSegment(segmentFile);

                // Commit successful
                return true;
            } catch (IOException e) {
                LOG.error("Error while committing segment data stored on shared filesystem.", e);

                // Commit failed
                return false;
            } finally {
                // Finally cleanup all files created on the local filesystem
                if (segmentData != null) {
                    segmentData.delete();
                }
                if (segmentFile != null) {
                    segmentFile.delete();
                }
            }
        }

        /**
         * Creates a segment from the given parameters.
         * This method was adapted from {@link org.apache.pinot.tools.admin.command.CreateSegmentCommand}.
         *
         * @param dataFile                  File containing the JSON data
         * @param outDir                    Segment target path
         * @param _postCreationVerification Verify segment after generation
         */
        private void generateSegment(File dataFile, File outDir, Boolean _postCreationVerification) {
            SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, tableSchema);
            segmentGeneratorConfig.setSegmentName(segmentName);
            segmentGeneratorConfig.setSegmentTimeUnit(segmentTimeUnit);
            segmentGeneratorConfig.setTimeColumnName(timeColumnName);
            segmentGeneratorConfig.setInputFilePath(dataFile.getPath());
            segmentGeneratorConfig.setFormat(FileFormat.JSON);
            segmentGeneratorConfig.setOutDir(outDir.getPath());
            segmentGeneratorConfig.setTableName(tableConfig.getTableName());

            try {
                SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
                driver.init(segmentGeneratorConfig);
                driver.build();
                File indexDir = new File(outDir, segmentName);
                LOG.debug("Successfully created segment: {} in directory: {}", segmentName, indexDir);
                if (_postCreationVerification) {
                    LOG.debug("Verifying the segment by loading it");
                    ImmutableSegment segment = ImmutableSegmentLoader.load(indexDir, ReadMode.mmap);
                    LOG.debug("Successfully loaded segment: {} of size: {} bytes", segmentName,
                            segment.getSegmentSizeBytes());
                    segment.destroy();
                }
            }
            // SegmentIndexCreationDriverImpl throws generic Exceptions during init and build
            // ImmutableSegmentLoader throws generic Exception during load
            catch (Exception e) {
                String message = String.format("Error while generating segment from file %s", dataFile.getAbsolutePath());
                LOG.error(message, e);
                throw new RuntimeException(message);
            }
            LOG.debug("Successfully created 1 segment from data file: {}", dataFile);
        }

        /**
         * Uploads a segment using the Pinot admin tool.
         *
         * @param segmentFile File containing the segment to upload
         * @throws IOException
         */
        private void uploadSegment(File segmentFile) throws IOException {
            try {
                UploadSegmentCommand cmd = new UploadSegmentCommand();
                cmd.setControllerHost(pinotControllerHost);
                cmd.setControllerPort(pinotControllerPort);
                cmd.setSegmentDir(segmentFile.getAbsolutePath());
                cmd.execute();
            } catch (Exception e) {
                // UploadSegmentCommand.execute() throws generic Exception
                LOG.error("Could not upload segment {}", segmentFile.getAbsolutePath(), e);
                throw new IOException(e.getMessage());
            }
        }
    }
}
