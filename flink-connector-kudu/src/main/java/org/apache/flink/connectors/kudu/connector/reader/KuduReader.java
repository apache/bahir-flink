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
package org.apache.flink.connectors.kudu.connector.reader;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.connectors.kudu.connector.KuduFilterInfo;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.convertor.RowResultConvertor;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Internal
public class KuduReader<T> implements AutoCloseable {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final KuduTableInfo tableInfo;
    private final KuduReaderConfig readerConfig;
    private List<KuduFilterInfo> tableFilters;
    private List<String> tableProjections;
    private final RowResultConvertor<T> rowResultConvertor;

    private final transient KuduClient client;
    private final transient KuduSession session;
    private final transient KuduTable table;

    public KuduReader(KuduTableInfo tableInfo, KuduReaderConfig readerConfig, RowResultConvertor<T> rowResultConvertor) throws IOException {
        this(tableInfo, readerConfig, rowResultConvertor, new ArrayList<>(), null);
    }

    public KuduReader(KuduTableInfo tableInfo, KuduReaderConfig readerConfig, RowResultConvertor<T> rowResultConvertor, List<KuduFilterInfo> tableFilters) throws IOException {
        this(tableInfo, readerConfig, rowResultConvertor, tableFilters, null);
    }

    public KuduReader(KuduTableInfo tableInfo, KuduReaderConfig readerConfig, RowResultConvertor<T> rowResultConvertor, List<KuduFilterInfo> tableFilters, List<String> tableProjections) throws IOException {
        this.tableInfo = tableInfo;
        this.readerConfig = readerConfig;
        this.tableFilters = tableFilters;
        this.tableProjections = tableProjections;
        this.rowResultConvertor = rowResultConvertor;
        this.client = obtainClient();
        this.session = obtainSession();
        this.table = obtainTable();
    }

    public void setTableFilters(List<KuduFilterInfo> tableFilters) {
        this.tableFilters = tableFilters;
    }

    public void setTableProjections(List<String> tableProjections) {
        this.tableProjections = tableProjections;
    }

    private KuduClient obtainClient() {
        return new KuduClient.KuduClientBuilder(readerConfig.getMasters())
                .build();
    }

    private KuduSession obtainSession() {
        return client.newSession();
    }

    private KuduTable obtainTable() throws IOException {
        String tableName = tableInfo.getName();
        if (client.tableExists(tableName)) {
            return client.openTable(tableName);
        }
        if (tableInfo.getCreateTableIfNotExists()) {
            return client.createTable(tableName, tableInfo.getSchema(), tableInfo.getCreateTableOptions());
        }
        throw new RuntimeException("Table " + tableName + " does not exist.");
    }

    public KuduReaderIterator<T> scanner(byte[] token) throws IOException {
        return new KuduReaderIterator<>(KuduScanToken.deserializeIntoScanner(token, client), rowResultConvertor);
    }

    public List<KuduScanToken> scanTokens(List<KuduFilterInfo> tableFilters, List<String> tableProjections, Integer rowLimit) {
        KuduScanToken.KuduScanTokenBuilder tokenBuilder = client.newScanTokenBuilder(table);

        if (tableProjections != null) {
            tokenBuilder.setProjectedColumnNames(tableProjections);
        }

        if (CollectionUtils.isNotEmpty(tableFilters)) {
            tableFilters.stream()
                    .map(filter -> filter.toPredicate(table.getSchema()))
                    .forEach(tokenBuilder::addPredicate);
        }

        if (rowLimit != null && rowLimit > 0) {
            tokenBuilder.limit(rowLimit);
        }

        return tokenBuilder.build();
    }

    public KuduInputSplit[] createInputSplits(int minNumSplits) throws IOException {

        List<KuduScanToken> tokens = scanTokens(tableFilters, tableProjections, readerConfig.getRowLimit());

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
            log.warn(" The minimum desired number of splits with your configured parallelism level " +
                            "is {}. Current kudu splits = {}. {} instances will remain idle.",
                    minNumSplits,
                    splits.length,
                    (minNumSplits - splits.length)
            );
        }

        return splits;
    }

    /**
     * Returns a endpoint url in the following format: <host>:<ip>
     *
     * @param host Hostname
     * @param port Port
     * @return Formatted URL
     */
    private String getLocation(String host, Integer port) {
        return host + ":" + port;
    }

    @Override
    public void close() throws IOException {
        try {
            if (session != null) {
                session.close();
            }
        } catch (KuduException e) {
            log.error("Error while closing session.", e);
        }
        try {
            if (client != null) {
                client.close();
            }
        } catch (Exception e) {
            log.error("Error while closing client.", e);
        }
    }
}

