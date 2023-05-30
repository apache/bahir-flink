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
package org.apache.flink.connectors.kudu.format;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.io.LocatableInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.kudu.connector.KuduFilterInfo;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.convertor.RowResultConvertor;
import org.apache.flink.connectors.kudu.connector.reader.KuduInputSplit;
import org.apache.flink.connectors.kudu.connector.reader.KuduReader;
import org.apache.flink.connectors.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connectors.kudu.connector.reader.KuduReaderIterator;
import org.apache.flink.connectors.kudu.table.KuduCatalog;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.kudu.client.KuduException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Input format for reading the contents of a Kudu table (defined by the provided {@link KuduTableInfo}) in both batch
 * and stream programs. Rows of the Kudu table are mapped to {@link T} instances that can converted to other data
 * types by the user later if necessary.
 *
 * <p> For programmatic access to the schema of the input rows users can use the {@link KuduCatalog}
 * or overwrite the column order manually by providing a list of projected column names.
 */
@PublicEvolving
public abstract class AbstractKuduInputFormat<T> extends RichInputFormat<T, KuduInputSplit> implements ResultTypeQueryable<T> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final KuduReaderConfig readerConfig;
    private final KuduTableInfo tableInfo;
    private final List<KuduFilterInfo> tableFilters;
    private final List<String> tableProjections;
    private final RowResultConvertor<T> rowResultConvertor;
    private boolean endReached;
    private transient KuduReader<T> kuduReader;
    private transient KuduReaderIterator<T> resultIterator;

    public AbstractKuduInputFormat(KuduReaderConfig readerConfig, RowResultConvertor<T> rowResultConvertor,
                                   KuduTableInfo tableInfo) {
        this(readerConfig, rowResultConvertor, tableInfo, new ArrayList<>(), null);
    }

    public AbstractKuduInputFormat(KuduReaderConfig readerConfig, RowResultConvertor<T> rowResultConvertor,
                                   KuduTableInfo tableInfo, List<String> tableProjections) {
        this(readerConfig, rowResultConvertor, tableInfo, new ArrayList<>(), tableProjections);
    }

    public AbstractKuduInputFormat(KuduReaderConfig readerConfig, RowResultConvertor<T> rowResultConvertor,
                                   KuduTableInfo tableInfo, List<KuduFilterInfo> tableFilters,
                                   List<String> tableProjections) {

        this.readerConfig = checkNotNull(readerConfig, "readerConfig could not be null");
        this.rowResultConvertor = checkNotNull(rowResultConvertor, "rowResultConvertor could not be null");
        this.tableInfo = checkNotNull(tableInfo, "tableInfo could not be null");
        this.tableFilters = checkNotNull(tableFilters, "tableFilters could not be null");
        this.tableProjections = tableProjections;

        this.endReached = false;
    }

    @Override
    public void configure(Configuration parameters) {
    }

    @Override
    public void open(KuduInputSplit split) throws IOException {
        endReached = false;
        startKuduReader();

        resultIterator = kuduReader.scanner(split.getScanToken());
    }

    private void startKuduReader() throws IOException {
        if (kuduReader == null) {
            kuduReader = new KuduReader<>(tableInfo, readerConfig, rowResultConvertor, tableFilters, tableProjections);
        }
    }

    private void closeKuduReader() throws IOException {
        if (kuduReader != null) {
            kuduReader.close();
            kuduReader = null;
        }
    }

    @Override
    public void close() throws IOException {
        if (resultIterator != null) {
            try {
                resultIterator.close();
            } catch (KuduException e) {
                e.printStackTrace();
            }
        }
        closeKuduReader();
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return cachedStatistics;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(KuduInputSplit[] inputSplits) {
        return new LocatableInputSplitAssigner(inputSplits);
    }

    @Override
    public KuduInputSplit[] createInputSplits(int minNumSplits) throws IOException {
        try {
            startKuduReader();
            return kuduReader.createInputSplits(minNumSplits);
        } finally {
            closeKuduReader();
        }
    }

    @Override
    public boolean reachedEnd() {
        return endReached;
    }

    @Override
    public T nextRecord(T reuse) throws IOException {
        // check that current iterator has next rows
        if (this.resultIterator.hasNext()) {
            return resultIterator.next();
        } else {
            endReached = true;
            return null;
        }
    }
}
