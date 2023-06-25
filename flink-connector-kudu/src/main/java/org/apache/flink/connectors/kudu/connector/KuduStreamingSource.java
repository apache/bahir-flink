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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.map.LinkedMap;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.kudu.connector.assigner.KuduDataSplitsAssigner;
import org.apache.flink.connectors.kudu.connector.configuration.StreamingColumn;
import org.apache.flink.connectors.kudu.connector.configuration.UserTableDataQueryDetail;
import org.apache.flink.connectors.kudu.connector.configuration.type.UserTableDataQueryFilter;
import org.apache.flink.connectors.kudu.connector.convertor.UserTableDataRowResultConvertor;
import org.apache.flink.connectors.kudu.connector.discover.KuduDataSplitsDiscoverer;
import org.apache.flink.connectors.kudu.connector.reader.KuduReader;
import org.apache.flink.connectors.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connectors.kudu.connector.reader.KuduReaderIterator;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.kudu.shaded.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Kudu source connector which provides the data continuously in the kudu table.
 * The connector will be running under the two modes:
 *
 * 1. {@link KuduStreamingRunningMode#CUSTOM_QUERY} the source connector will fetch the records
 * in kudu table with logic of the user provided
 *
 * 2. {@link KuduStreamingRunningMode#INCREMENTAL} the source connector will fetch the records
 * in the kudu table by the order of the fields annotated {@link StreamingColumn}. This assumes
 * that the fields are unique and increased monotonically, e.g. db auto incremented identifier.
 * To promise the order, only one subtask will be running and sorted the records before emitting.
 *
 * @param <T> The mapped Java type against the Kudu table.
 */
@PublicEvolving
public class KuduStreamingSource<T> extends RichParallelSourceFunction<T>
        implements CheckpointListener, CheckpointedFunction {

    private static final long serialVersionUID = -2527403358494874319L;

    private final KuduStreamingSourceConfiguration<T> kuduStreamingSourceConfiguration;

    private transient KuduTableInfo tableInfo;

    private transient Long batchRunningInterval = 10000L;

    private transient UserTableDataRowResultConvertor<T> rowResultConvertor;

    private transient StreamingLocalEventsManager<T> streamingKeyOffsetManager;

    private volatile boolean running = true;

    private transient ListState<LinkedMap> snapshotOffsetStates;
    private static final String SNAPSHOT_OFFSET_STATES_KEY = "snapshot-offset-states";

    private LinkedMap inflightOffsets = new LinkedMap();

    private static final Logger LOGGER = LoggerFactory.getLogger(KuduStreamingSource.class);

    public KuduStreamingSource(KuduStreamingSourceConfiguration<T> kuduStreamingSourceConfiguration) {
        this.kuduStreamingSourceConfiguration = kuduStreamingSourceConfiguration;
    }

    private Object getTypeValue(Class<?> clz, String value) {
        if (clz == Long.class) {
            return Long.valueOf(value);
        } else if (clz == Integer.class) {
            return Integer.valueOf(value);
        } else if (clz == Short.class) {
            return Short.valueOf(value);
        } else if (clz == Byte.class) {
            return Byte.valueOf(value);
        } else if (clz == String.class){
            return value;
        } else {
            throw new IllegalArgumentException("Unsupported type: " + clz + " for streaming key.");
        }
    }

    @Override
    public void run(SourceContext<T> sourceContext) {
        while (running) {
            LOGGER.info("Running the kudu source connector ...");
            KuduReader<T> kuduReader = null;
            try {
                KuduReaderConfig.Builder kuduReaderConfigBuilder = KuduReaderConfig.Builder
                        .setMasters(kuduStreamingSourceConfiguration.getMasterAddresses());
                KuduReaderConfig readerConfig = kuduReaderConfigBuilder.build();

                kuduReader = new KuduReader<>(tableInfo, readerConfig, rowResultConvertor);

                List<KuduFilterInfo> filterInfoList = Lists.newArrayList();
                List<String> projectedColumnList = null;
                if (CollectionUtils.isNotEmpty(kuduStreamingSourceConfiguration.getUserTableDataQueryDetailList())) {

                    List<UserTableDataQueryDetail> allUserTableDataQueryDetails =
                            kuduStreamingSourceConfiguration.getUserTableDataQueryDetailList();
                    UserTableDataQueryDetail userTableDataQueryDetail = allUserTableDataQueryDetails.get(0);

                    if (kuduStreamingSourceConfiguration.getRunningMode() == KuduStreamingRunningMode.INCREMENTAL) {
                        String[] streamingLowerKey = streamingKeyOffsetManager.getCurrentHWM();
                        String[] streamingUpperKey = streamingKeyOffsetManager.getUserConfiguredUpperKey();
                        List<StreamingColumn> streamingColumns = rowResultConvertor.getUserTableDataTypeDetail().getStreamingCols();
                        // Build the lower and upper bound
                        for (int i = 0; i < streamingColumns.size(); i++) {
                            LOGGER.info("STREAMING_LOWER_KEY={}, STREAMING_UPPER_KEY={}", Arrays.toString(streamingLowerKey), Arrays.toString(streamingUpperKey));
                            StreamingColumn streamingColumn = streamingColumns.get(i);
                            if (streamingLowerKey != null) {
                                KuduFilterInfo lowerFilterInfo = KuduFilterInfo.Builder
                                        .create(streamingColumn.getColName()).filter(
                                                i == 0 ? KuduFilterInfo.FilterType.GREATER : KuduFilterInfo.FilterType.GREATER_EQUAL,
                                                getTypeValue(streamingColumn.getFieldType(), streamingLowerKey[i])).build();

                                filterInfoList.add(lowerFilterInfo);
                            }

                            if (streamingUpperKey != null) {
                                KuduFilterInfo upperFilterInfo = KuduFilterInfo.Builder
                                        .create(streamingColumn.getColName()).filter(
                                                KuduFilterInfo.FilterType.LESS_EQUAL,
                                                getTypeValue(streamingColumn.getFieldType(), streamingUpperKey[i])).build();

                                filterInfoList.add(upperFilterInfo);
                            }
                        }
                    } else {
                        if (CollectionUtils.isNotEmpty(userTableDataQueryDetail.getUserTableDataQueryFilters())) {
                            List<KuduFilterInfo> tableFilters = Lists.newArrayList();
                            for (UserTableDataQueryFilter filterDetail :
                                    userTableDataQueryDetail.getUserTableDataQueryFilters()) {
                                KuduFilterInfo filterInfo = KuduFilterInfo.Builder
                                        .create(filterDetail.getColName()).filter(
                                                filterDetail.getFilterOp().getKuduFilterType(),
                                                filterDetail.getFilterValueResolver().resolve())
                                        .build();

                                tableFilters.add(filterInfo);
                            }

                            filterInfoList = tableFilters;
                        }
                    }

                    kuduReader.setTableFilters(filterInfoList);

                    if (CollectionUtils.isNotEmpty(userTableDataQueryDetail.getProjectedColumns())) {
                        projectedColumnList = userTableDataQueryDetail.getProjectedColumns();
                    }

                    kuduReader.setTableProjections(projectedColumnList);
                }

                KuduDataSplitsDiscoverer kuduDataSplitsDiscoverer = KuduDataSplitsDiscoverer.builder()
                        .reader(kuduReader)
                        .filterInfoList(filterInfoList)
                        .projectedColumnList(projectedColumnList)
                        .build();

                List<KuduDataSplit> dataSplits = kuduDataSplitsDiscoverer.getAllKuduDataSplits();
                List<KuduDataSplit> assignedSplits = Lists.newArrayList();
                int thisSubTaskId = getRuntimeContext().getIndexOfThisSubtask();
                int totalSubTask = getRuntimeContext().getNumberOfParallelSubtasks();
                for (KuduDataSplit split : dataSplits) {
                    int assignedSubtaskId = KuduDataSplitsAssigner.assign(
                            split, totalSubTask,
                            kuduStreamingSourceConfiguration.getRunningMode());
                    LOGGER.info("TASK_ASSIGNED, totalSubTask={}, assignedSubTaskId={}, thisSubTaskId={}",
                            totalSubTask, assignedSubtaskId, thisSubTaskId);
                    if (assignedSubtaskId == thisSubTaskId) {
                        assignedSplits.add(split);
                    }
                }

                for (KuduDataSplit split : assignedSplits) {
                    KuduReaderIterator<T> resultIterator = kuduReader.scanner(split.getScanToken());
                    while (resultIterator.hasNext()) {
                        T row = resultIterator.next();
                        if (row != null) {
                            /** For the running mode == KuduStreamingRunningMode#INCREMENTAL, we need to manage the offsets of the table.
                             * The data will be in the local buffer and sorted before emitting.
                             */
                            if (kuduStreamingSourceConfiguration.getRunningMode() == KuduStreamingRunningMode.INCREMENTAL) {
                                streamingKeyOffsetManager.update(row);
                            } else {
                                sourceContext.collect(row);
                            }
                        }
                    }
                }

                if (kuduStreamingSourceConfiguration.getRunningMode() == KuduStreamingRunningMode.INCREMENTAL) {
                    Iterator<T> eventItr = streamingKeyOffsetManager.getSortedLocalEvents().iterator();
                    while (eventItr.hasNext()) {
                        sourceContext.collect(eventItr.next());
                    }

                    streamingKeyOffsetManager.next();
                }

                Thread.sleep(batchRunningInterval);
            } catch (Exception e) {
                LOGGER.error("Exception happened when reading records", e);
                throw new RuntimeException(e);
            } finally {
                try {
                    if (kuduReader != null) {
                        kuduReader.close();
                    }
                } catch (Exception e) {
                    LOGGER.error("Error on closing kuduReader", e);
                }
            }
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        tableInfo = KuduTableInfo.forTable(kuduStreamingSourceConfiguration.getTableName());

        if (rowResultConvertor == null) {
            rowResultConvertor =
                    new UserTableDataRowResultConvertor<>(kuduStreamingSourceConfiguration.getTargetKuduRowClz());
        }

        if (streamingKeyOffsetManager == null) {
            String userConfiguredLowerKey =
                    CollectionUtils.isNotEmpty(kuduStreamingSourceConfiguration.getUserTableDataQueryDetailList()) ?
                            kuduStreamingSourceConfiguration.getUserTableDataQueryDetailList().get(0).getLowerBoundKey() : null;
            String userConfiguredUpperKey =
                    CollectionUtils.isNotEmpty(kuduStreamingSourceConfiguration.getUserTableDataQueryDetailList()) ?
                            kuduStreamingSourceConfiguration.getUserTableDataQueryDetailList().get(0).getUpperBoundKey() : null;

            streamingKeyOffsetManager = new StreamingLocalEventsManager<>(
                    rowResultConvertor.getUserTableDataTypeDetail().getStreamingCols(), userConfiguredLowerKey, userConfiguredUpperKey);
        }

        if (kuduStreamingSourceConfiguration.getBatchRunningInterval() != null) {
            batchRunningInterval = kuduStreamingSourceConfiguration.getBatchRunningInterval();
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public void notifyCheckpointComplete(long l) {
        if (kuduStreamingSourceConfiguration.getRunningMode() == KuduStreamingRunningMode.INCREMENTAL) {
            // Mark the state as committed
            IncrementalCPState incrementalCPState = (IncrementalCPState) inflightOffsets.get(l);
            incrementalCPState.setCommitted(true);

            int position = inflightOffsets.indexOf(l);

            LOGGER.info("NOTIFY_CHECKPOINT, checkpointId={}, subTaskId={}, position={}",
                    l, getRuntimeContext().getIndexOfThisSubtask(), position);
            // Remove the stale entries
            if (position != -1) {
                for (int i = 0; i < position; i++) {
                    inflightOffsets.remove(0);
                }
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        if (kuduStreamingSourceConfiguration.getRunningMode() == KuduStreamingRunningMode.INCREMENTAL) {
            snapshotOffsetStates.clear();

            IncrementalCPState incrementalCPState = IncrementalCPState.builder()
                    .subTaskId(getRuntimeContext().getIndexOfThisSubtask())
                    .checkpointId(functionSnapshotContext.getCheckpointId())
                    .offset(streamingKeyOffsetManager.getCurrentHWMStr())
                    .build();

            LOGGER.info("SNAPSHOT_STATE for checkpointId={}, subTask={}, with state={}",
                    functionSnapshotContext.getCheckpointId(), getRuntimeContext().getIndexOfThisSubtask(), incrementalCPState);

            inflightOffsets.put(functionSnapshotContext.getCheckpointId(), incrementalCPState);

            snapshotOffsetStates.add(inflightOffsets);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        if (kuduStreamingSourceConfiguration.getRunningMode() == KuduStreamingRunningMode.INCREMENTAL) {
            snapshotOffsetStates = functionInitializationContext.getOperatorStateStore().getUnionListState(
                    new ListStateDescriptor<>(
                            SNAPSHOT_OFFSET_STATES_KEY, TypeInformation.of(new TypeHint<LinkedMap>() {
                    })));
            LOGGER.info("INIT_STATE for subTask={}, with state={}", getRuntimeContext().getIndexOfThisSubtask(), snapshotOffsetStates);
            if (functionInitializationContext.isRestored()) {
                rowResultConvertor =
                        new UserTableDataRowResultConvertor<>(kuduStreamingSourceConfiguration.getTargetKuduRowClz());
                String userConfiguredLowerKey =
                        CollectionUtils.isNotEmpty(kuduStreamingSourceConfiguration.getUserTableDataQueryDetailList()) ?
                                kuduStreamingSourceConfiguration.getUserTableDataQueryDetailList().get(0).getLowerBoundKey() : null;
                String userConfiguredUpperKey =
                        CollectionUtils.isNotEmpty(kuduStreamingSourceConfiguration.getUserTableDataQueryDetailList()) ?
                                kuduStreamingSourceConfiguration.getUserTableDataQueryDetailList().get(0).getUpperBoundKey() : null;
                streamingKeyOffsetManager = new StreamingLocalEventsManager<>(
                        rowResultConvertor.getUserTableDataTypeDetail().getStreamingCols(), userConfiguredLowerKey, userConfiguredUpperKey);

                for (LinkedMap state : snapshotOffsetStates.get()) {
                    LOGGER.info("RESTORE_STATE for subTask={}, with state={}", getRuntimeContext().getIndexOfThisSubtask(), state);
                    // Find the latest committed offset for the current subTask
                    for  (int i = 0; i < state.size(); i++) {
                        IncrementalCPState incrementalCPState = (IncrementalCPState)state.getValue(i);
                        if (incrementalCPState.isCommitted()) {
                            LOGGER.info("RESTORE_STATE for subTask={}, with incrementalCPState={}", getRuntimeContext().getIndexOfThisSubtask(), incrementalCPState);
                            if (getRuntimeContext().getIndexOfThisSubtask() == incrementalCPState.getSubTaskId()) {
                                LOGGER.info("SET_INIT_HWM={}", incrementalCPState.getOffset());
                                streamingKeyOffsetManager.setInitialHWM(incrementalCPState.getOffset());
                            }
                        }
                    }
                }
                LOGGER.info("FINAL_INIT_HWM={}", streamingKeyOffsetManager.getCurrentHWMStr());
            }
        }
    }
}
