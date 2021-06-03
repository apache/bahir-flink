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
package org.apache.flink.connectors.kudu.table;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.*;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

/**
 * @author weijunhao
 * @date 2021/5/28 16:55
 */
public class KuduDynamicTableSource implements ScanTableSource, LookupTableSource {

    private final String master;
    private final String tableName;
    private final TableSchema physicalSchema;
    private final LookupOptions lookupOptions;
    private final ScanOptions scanOptions;

    public KuduDynamicTableSource(String master,
                                  String tableName,
                                  TableSchema physicalSchema,
                                  LookupOptions lookupOptions,
                                  ScanOptions scanOptions) {
        this.master = master;
        this.tableName = tableName;
        this.physicalSchema = physicalSchema;
        this.lookupOptions = lookupOptions;
        this.scanOptions = scanOptions;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        SourceFunctionProvider provider =
                SourceFunctionProvider.of(new KuduDynamicTableSourceProvider(master, tableName, scanOptions), false);
        return provider;
    }


    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        // only support non-nested look up keys
        String[] keyNames = new String[context.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "only support non-nested look up keys");
            keyNames[i] = physicalSchema.getFieldNames()[innerKeyArr[0]];
        }
        TableFunctionProvider<RowData> provider = TableFunctionProvider.of(
                new KuduLookupFunction(
                        master,
                        tableName,
                        keyNames,
                        lookupOptions.getCacheMaxSize(),
                        lookupOptions.getCacheExpireMs(),
                        lookupOptions.getMaxRetryTimes()));
        return provider;
    }

    @Override
    public DynamicTableSource copy() {
        return new KuduDynamicTableSource(master, tableName, physicalSchema, lookupOptions, scanOptions);
    }

    @Override
    public String asSummaryString() {
        return String.format("Kudu[%s]", tableName);
    }

    public static class LookupOptions {
        private final long cacheMaxSize;
        private final long cacheExpireMs;
        private final int maxRetryTimes;

        public LookupOptions(long cacheMaxSize, long cacheExpireMs, int maxRetryTimes) {
            this.cacheMaxSize = cacheMaxSize;
            this.cacheExpireMs = cacheExpireMs;
            this.maxRetryTimes = maxRetryTimes;
        }

        public long getCacheMaxSize() {
            return cacheMaxSize;
        }

        public long getCacheExpireMs() {
            return cacheExpireMs;
        }

        public int getMaxRetryTimes() {
            return maxRetryTimes;
        }
    }

    public static class ScanOptions {
        private final long inerval;

        public ScanOptions(long inerval) {
            this.inerval = inerval;
        }

        public long getInerval() {
            return inerval;
        }
    }

}