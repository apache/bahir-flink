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
package org.apache.flink.streaming.connectors.kudu.connector;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.time.Time;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class KuduConnector implements AutoCloseable {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private Callback<Boolean, OperationResponse> defaultCB;

    public enum Consistency {EVENTUAL, STRONG};
    public enum WriteMode {INSERT,UPDATE,UPSERT}

    private AsyncKuduClient client;
    private KuduTable table;

    private Consistency consistency;
    private WriteMode writeMode;

    private static AtomicInteger pendingTransactions = new AtomicInteger();
    private static AtomicBoolean errorTransactions = new AtomicBoolean(false);
    private Callback<Boolean, OperationResponse> callback;

    public KuduConnector(String kuduMasters, KuduTableInfo tableInfo) throws IOException {
        this(kuduMasters, tableInfo, KuduConnector.Consistency.STRONG, KuduConnector.WriteMode.UPSERT);
    }

    public KuduConnector(String kuduMasters, KuduTableInfo tableInfo, Consistency consistency, WriteMode writeMode) throws IOException {
        this.client = client(kuduMasters);
        this.table = table(tableInfo);
        this.consistency = consistency;
        this.writeMode = writeMode;
        this.defaultCB = new ResponseCallback();
    }

    private AsyncKuduClient client(String kuduMasters) {
        return new AsyncKuduClient.AsyncKuduClientBuilder(kuduMasters).build();
    }

    private KuduTable table(KuduTableInfo infoTable) throws IOException {
        KuduClient syncClient = client.syncClient();

        String tableName = infoTable.getName();
        if (syncClient.tableExists(tableName)) {
            return syncClient.openTable(tableName);
        }
        if (infoTable.createIfNotExist()) {
            return syncClient.createTable(tableName, infoTable.getSchema(), infoTable.getCreateTableOptions());
        }
        throw new UnsupportedOperationException("table not exists and is marketed to not be created");
    }

    public boolean deleteTable() throws IOException {
        String tableName = table.getName();
        client.syncClient().deleteTable(tableName);
        return true;
    }

    public KuduRowIterator scanner(byte[] token) throws IOException {
        return new KuduRowIterator(KuduScanToken.deserializeIntoScanner(token, client.syncClient()));
    }

    public List<KuduScanToken> scanTokens(List<KuduFilterInfo> tableFilters, List<String> tableProjections, Long rowLimit) {
        KuduScanToken.KuduScanTokenBuilder tokenBuilder = client.syncClient().newScanTokenBuilder(table);

        if (CollectionUtils.isNotEmpty(tableProjections)) {
            tokenBuilder.setProjectedColumnNames(tableProjections);
        }

        if (CollectionUtils.isNotEmpty(tableFilters)) {
            tableFilters.stream()
                    .map(filter -> filter.toPredicate(table.getSchema()))
                    .forEach(tokenBuilder::addPredicate);
        }

        if (rowLimit !=null && rowLimit > 0) {
            tokenBuilder.limit(rowLimit);
        }

        return tokenBuilder.build();
    }

    public boolean writeRow(KuduRow row) throws Exception {
        final Operation operation = KuduMapper.toOperation(table, writeMode, row);

        AsyncKuduSession session = client.newSession();
        Deferred<OperationResponse> response = session.apply(operation);

        if (KuduConnector.Consistency.EVENTUAL.equals(consistency)) {
            pendingTransactions.incrementAndGet();
            response.addCallback(callback);
        } else {
            processResponse(response.join());
        }

        session.close();
        return !errorTransactions.get();

    }

    @Override
    public void close() throws Exception {
        while(pendingTransactions.get() > 0) {
            LOG.info("sleeping {}s by pending transactions", pendingTransactions.get());
            Thread.sleep(Time.seconds(pendingTransactions.get()).toMilliseconds());
        }

        if (client == null) return;
        client.close();
    }

    private class ResponseCallback implements Callback<Boolean, OperationResponse> {
        @Override
        public Boolean call(OperationResponse operationResponse) {
            processResponse(operationResponse);
            return errorTransactions.get();
        }
    }

    protected void processResponse(OperationResponse operationResponse) {
        if (operationResponse == null) return;

        if (operationResponse.hasRowError()) {
            logResponseError(operationResponse.getRowError());
            errorTransactions.set(true);
        }
    }

    private void logResponseError(RowError error) {
        LOG.error("Error {} on {}: {} ", error.getErrorStatus(), error.getOperation(), error.toString());
    }

}
