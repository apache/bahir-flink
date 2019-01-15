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
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class KuduConnector implements AutoCloseable {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private Callback<Boolean, OperationResponse> defaultCB;

    public enum Consistency {EVENTUAL, STRONG};
    public enum WriteMode {INSERT,UPDATE,UPSERT}

    private AsyncKuduClient client;
    private KuduTable table;

    public KuduConnector(String kuduMasters, KuduTableInfo tableInfo) throws IOException {
        client = client(kuduMasters);
        table = table(tableInfo);
        defaultCB = new ResponseCallback();
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

    public Deferred<OperationResponse> writeRow(KuduRow row, WriteMode writeMode) throws Exception {
        final Operation operation = KuduMapper.toOperation(table, writeMode, row);

        AsyncKuduSession session = client.newSession();
        Deferred<OperationResponse> response = session.apply(operation);
        session.close();
        return response;
    }

    @Override
    public void close() throws Exception {
        if (client == null) return;

        client.close();
    }

    private class ResponseCallback implements Callback<Boolean, OperationResponse> {
        @Override
        public Boolean call(OperationResponse operationResponse) {
            return processResponse(operationResponse);
        }

        private Boolean processResponse(OperationResponse operationResponse) {
            if (operationResponse == null) return true;
            logResponseError(operationResponse.getRowError());
            return operationResponse.hasRowError();
        }

        private void logResponseError(RowError error) {
            LOG.error("Error {} on {}: {} ", error.getErrorStatus(), error.getOperation(), error.toString());
        }
    }

}
