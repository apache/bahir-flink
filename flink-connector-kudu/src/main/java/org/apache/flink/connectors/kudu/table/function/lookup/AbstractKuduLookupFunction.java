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

package org.apache.flink.connectors.kudu.table.function.lookup;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.connectors.kudu.connector.KuduFilterInfo;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.convertor.RowResultConvertor;
import org.apache.flink.connectors.kudu.connector.reader.KuduInputSplit;
import org.apache.flink.connectors.kudu.connector.reader.KuduReader;
import org.apache.flink.connectors.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connectors.kudu.connector.reader.KuduReaderIterator;
import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * A {@link TableFunction} to query fields from Kudu by keys.
 * The query template like:
 * <PRE>
 * SELECT c, d, e, f from T where a = ? and b = ?
 * </PRE>
 *
 * <p>Support cache the result to avoid frequent accessing to remote databases.
 * 1.The cacheMaxSize is -1 means not use cache.
 * 2.For real-time data, you need to set the TTL of cache.
 *
 * @param <IN> Type of the input records
 */
public abstract class AbstractKuduLookupFunction<IN> extends TableFunction<IN> {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractKuduLookupFunction.class);
    private static final long serialVersionUID = 1L;

    private final KuduTableInfo tableInfo;
    private final KuduReaderConfig kuduReaderConfig;
    private final String[] keyNames;
    private final String[] projectedFields;
    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private final int maxRetryTimes;
    private final RowResultConvertor<IN> convertor;

    private transient Cache<IN, List<IN>> cache;
    private transient KuduReader<IN> kuduReader;
    private transient Integer keyCount = 0;

    public AbstractKuduLookupFunction(String[] keyNames, RowResultConvertor<IN> convertor, KuduTableInfo tableInfo,
                                      KuduReaderConfig kuduReaderConfig, String[] projectedFields,
                                      KuduLookupOptions kuduLookupOptions) {
        this.tableInfo = tableInfo;
        this.convertor = convertor;
        this.projectedFields = projectedFields;
        this.keyNames = keyNames;
        this.kuduReaderConfig = kuduReaderConfig;
        this.cacheMaxSize = kuduLookupOptions.getCacheMaxSize();
        this.cacheExpireMs = kuduLookupOptions.getCacheExpireMs();
        this.maxRetryTimes = kuduLookupOptions.getMaxRetryTimes();
    }

    /**
     * Template method to build cache key
     *
     * @param keys join keys
     * @return cache key
     */
    public abstract IN buildCacheKey(Object... keys);

    /**
     * invoke entry point of lookup function.
     *
     * @param keys join keys
     */
    public void eval(Object... keys) {
        if (keys.length != keyNames.length) {
            throw new RuntimeException("The join keys are of unequal lengths");
        }
        // cache key
        IN keyRow = buildCacheKey(keys);
        if (this.cache != null) {
            ConcurrentMap<IN, List<IN>> cacheMap = this.cache.asMap();
            this.keyCount = cacheMap.size();
            List<IN> cacheRows = this.cache.getIfPresent(keyRow);
            if (CollectionUtils.isNotEmpty(cacheRows)) {
                for (IN cacheRow : cacheRows) {
                    collect(cacheRow);
                }
                return;
            }
        }

        for (int retry = 1; retry <= maxRetryTimes; retry++) {
            try {
                List<KuduFilterInfo> kuduFilterInfos = buildKuduFilterInfo(keys);
                this.kuduReader.setTableFilters(kuduFilterInfos);
                KuduInputSplit[] inputSplits = kuduReader.createInputSplits(1);
                ArrayList<IN> rows = new ArrayList<>();
                for (KuduInputSplit inputSplit : inputSplits) {
                    KuduReaderIterator<IN> scanner = kuduReader.scanner(inputSplit.getScanToken());
                    // 没有启用cache
                    if (cache == null) {
                        while (scanner.hasNext()) {
                            collect(scanner.next());
                        }
                    } else {
                        while (scanner.hasNext()) {
                            IN row = scanner.next();
                            rows.add(row);
                            collect(row);
                        }
                        rows.trimToSize();
                    }
                }
                if (cache != null) {
                    cache.put(keyRow, rows);
                }
                break;
            } catch (Exception e) {
                LOG.error(String.format("Kudu scan error, retry times = %d", retry), e);
                if (retry >= maxRetryTimes) {
                    throw new RuntimeException("Execution of Kudu scan failed.", e);
                }
                try {
                    Thread.sleep(1000L * retry);
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            }
        }
    }

    /**
     * build kuduFilterInfo
     *
     * @return kudu filters
     */
    private List<KuduFilterInfo> buildKuduFilterInfo(Object... keyValS) {
        List<KuduFilterInfo> kuduFilterInfos = Lists.newArrayList();
        for (int i = 0; i < keyNames.length; i++) {
            KuduFilterInfo kuduFilterInfo = KuduFilterInfo.Builder.create(keyNames[i])
                    .equalTo(keyValS[i]).build();
            kuduFilterInfos.add(kuduFilterInfo);
        }
        return kuduFilterInfos;
    }


    @Override
    public void open(FunctionContext context) {
        try {
            super.open(context);
            this.kuduReader = new KuduReader<>(this.tableInfo, this.kuduReaderConfig, this.convertor);
            // build kudu cache
            this.kuduReader.setTableProjections(ArrayUtils.isNotEmpty(projectedFields) ?
                    Arrays.asList(projectedFields) : null);
            this.cache = this.cacheMaxSize == -1 || this.cacheExpireMs == -1 ? null : CacheBuilder.newBuilder()
                    .expireAfterWrite(this.cacheExpireMs, TimeUnit.MILLISECONDS)
                    .maximumSize(this.cacheMaxSize)
                    .build();
        } catch (Exception ioe) {
            LOG.error("Exception while creating connection to Kudu.", ioe);
            throw new RuntimeException("Cannot create connection to Kudu.", ioe);
        }
    }

    @Override
    public void close() {
        if (null != this.kuduReader) {
            try {
                this.kuduReader.close();
                this.cache.cleanUp();
                // help gc
                this.cache = null;
                this.kuduReader = null;
            } catch (IOException e) {
                // ignore exception when close.
                LOG.warn("exception when close table", e);
            }
        }
    }
}