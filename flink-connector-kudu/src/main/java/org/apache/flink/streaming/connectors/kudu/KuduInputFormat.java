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
package org.apache.flink.streaming.connectors.kudu;

import org.apache.flink.api.common.io.LocatableInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.core.io.LocatableInputSplit;
import org.apache.flink.streaming.connectors.kudu.connector.*;
import org.apache.flink.util.Preconditions;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KuduInputFormat extends RichInputFormat<KuduRow, KuduInputFormat.KuduInputSplit> {

    private String kuduMasters;
    private KuduTableInfo tableInfo;
    private List<KuduFilterInfo> tableFilters;
    private List<String> tableProjections;
    private Long rowsLimit;
    private boolean endReached;

    private transient KuduConnector tableContext;
    private transient KuduScanner scanner;
    private transient RowResultIterator resultIterator;

    private static final Logger LOG = LoggerFactory.getLogger(KuduInputFormat.class);

    public KuduInputFormat(String kuduMasters, KuduTableInfo tableInfo) {
        Preconditions.checkNotNull(kuduMasters,"kuduMasters could not be null");
        this.kuduMasters = kuduMasters;

        Preconditions.checkNotNull(tableInfo,"tableInfo could not be null");
        this.tableInfo = tableInfo;

        this.endReached = false;
    }

    public KuduInputFormat withTableFilters(KuduFilterInfo... tableFilters) {
        return withTableFilters(Arrays.asList(tableFilters));
    }

    public KuduInputFormat withTableFilters(List<KuduFilterInfo> tableFilters) {
        this.tableFilters = tableFilters;
        return this;
    }

    public KuduInputFormat withTableProjections(String... tableProjections) {
        return withTableProjections(Arrays.asList(tableProjections));
    }
    public KuduInputFormat withTableProjections(List<String> tableProjections) {
        this.tableProjections = tableProjections;
        return this;
    }

    public KuduInputFormat withRowsLimit(Long rowsLimit) {
        this.rowsLimit = rowsLimit;
        return this;
    }

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public void open(KuduInputSplit split) throws IOException {
        endReached = false;
        startTableContext();

        scanner = tableContext.scanner(split.getScanToken());
        resultIterator = scanner.nextRows();
    }

    @Override
    public void close() {
        if (scanner != null) {
            try {
                scanner.close();
            } catch (KuduException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return cachedStatistics;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(KuduInputSplit[] inputSplits) {
        return new LocatableInputSplitAssigner(inputSplits);
    }

    private void startTableContext() throws IOException {
        if (tableContext == null) {
            tableContext = new KuduConnector(kuduMasters, tableInfo);
        }
    }

    @Override
    public KuduInputSplit[] createInputSplits(int minNumSplits) throws IOException {
        startTableContext();
        Preconditions.checkNotNull(tableContext,"tableContext should not be null");

        List<KuduScanToken> tokens = tableContext.scanTokens(tableFilters, tableProjections, rowsLimit);

        KuduInputSplit[] splits = new KuduInputSplit[tokens.size()];

        for (int i = 0; i < tokens.size(); i++) {
            KuduScanToken token = tokens.get(i);

            List<String> locations = new ArrayList<>(token.getTablet().getReplicas().size());

            for (LocatedTablet.Replica replica : token.getTablet().getReplicas()) {
                locations.add(getLocation(replica.getRpcHost(), replica.getRpcPort()));
            }

            KuduInputSplit split = new KuduInputSplit(
                    token.serialize(),
                    i,
                    locations.toArray(new String[locations.size()])
            );
            splits[i] = split;
        }

        if (splits.length < minNumSplits) {
            LOG.warn(" The minimum desired number of splits with your configured parallelism level " +
                            "is {}. Current kudu splits = {}. {} instances will remain idle.",
                    minNumSplits,
                    splits.length,
                    (minNumSplits - splits.length)
            );
        }

        return splits;
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return endReached;
    }

    @Override
    public KuduRow nextRecord(KuduRow reuse) throws IOException {
        // check that current iterator has next rows
        if (this.resultIterator.hasNext()) {
            RowResult row = this.resultIterator.next();
            return KuduMapper.toKuduRow(row);
        }
        // if not, check that current scanner has more iterators
        else if (scanner.hasMoreRows()) {
            this.resultIterator = scanner.nextRows();
            return nextRecord(reuse);
        }
        else {
            endReached = true;
        }
        return null;
    }

    /**
     * Returns a endpoint url in the following format: <host>:<ip>
     *
     * @param host Hostname
     * @param port Port
     * @return Formatted URL
     */
    private String getLocation(String host, Integer port) {
        StringBuilder builder = new StringBuilder();
        builder.append(host).append(":").append(port);
        return builder.toString();
    }


    public class KuduInputSplit extends LocatableInputSplit {

        private byte[] scanToken;

        /**
         * Creates a new KuduInputSplit
         * @param splitNumber the number of the input split
         * @param hostnames The names of the hosts storing the data this input split refers to.
         */
        public KuduInputSplit(byte[] scanToken, final int splitNumber, final String[] hostnames) {
            super(splitNumber, hostnames);

            this.scanToken = scanToken;
        }

        public byte[] getScanToken() {
            return scanToken;
        }
    }
}
