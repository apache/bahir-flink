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
package org.apache.flink.connectors.kudu.connector.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connectors.kudu.connector.KuduRow;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.failure.DefaultKuduFailureHandler;
import org.apache.flink.connectors.kudu.connector.failure.KuduFailureHandler;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@Internal
public class KuduWriter implements AutoCloseable {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final KuduTableInfo tableInfo;
    private final KuduWriterConfig writerConfig;
    private final KuduFailureHandler failureHandler;

    private transient KuduClient client;
    private transient KuduSession session;
    private transient KuduTable table;


    public KuduWriter(KuduTableInfo tableInfo, KuduWriterConfig writerConfig) throws IOException {
        this (tableInfo, writerConfig, new DefaultKuduFailureHandler());
    }
    public KuduWriter(KuduTableInfo tableInfo, KuduWriterConfig writerConfig, KuduFailureHandler failureHandler) throws IOException {
        this.tableInfo = tableInfo;
        this.writerConfig = writerConfig;
        this.failureHandler = failureHandler;

        this.client = obtainClient();
        this.session = obtainSession();
        this.table = obtainTable();
    }

    private KuduClient obtainClient() {
        return new KuduClient.KuduClientBuilder(writerConfig.getMasters()).build();
    }

    private KuduSession obtainSession() {
        KuduSession session = client.newSession();
        session.setFlushMode(writerConfig.getFlushMode());
        return session;
    }

    private KuduTable obtainTable() throws IOException {
        String tableName = tableInfo.getName();
        if (client.tableExists(tableName)) {
            return client.openTable(tableName);
        }
        if (tableInfo.createIfNotExist()) {
            return client.createTable(tableName, tableInfo.getSchema(), tableInfo.getCreateTableOptions());
        }
        throw new UnsupportedOperationException("table not exists and is marketed to not be created");
    }

    public Schema getSchema() {
        return table.getSchema();
    }

    public void write(KuduRow row) throws IOException {
        checkAsyncErrors();

        final Operation operation = mapToOperation(row);
        final OperationResponse response = session.apply(operation);

        checkErrors(response);
    }

    public void flushAndCheckErrors() throws IOException {
        checkAsyncErrors();
        flush();
        checkAsyncErrors();
    }

    @VisibleForTesting
    public DeleteTableResponse deleteTable() throws IOException {
        String tableName = table.getName();
        return client.deleteTable(tableName);
    }

    @Override
    public void close() throws IOException {
        try {
            flushAndCheckErrors();
        } finally {
            try {
                if (session != null) {
                    session.close();
                }
            } catch (Exception e) {
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

    private void flush() throws IOException {
        session.flush();
    }

    private void checkErrors(OperationResponse response) throws IOException {
        if (response != null && response.hasRowError()) {
            failureHandler.onFailure(Arrays.asList(response.getRowError()));
        } else {
            checkAsyncErrors();
        }
    }

    private void checkAsyncErrors() throws IOException {
        if (session.countPendingErrors() == 0) return;

        List<RowError> errors = Arrays.asList(session.getPendingErrors().getRowErrors());
        failureHandler.onFailure(errors);
    }

    private Operation mapToOperation(KuduRow row) {
        final Operation operation = obtainOperation();
        final PartialRow partialRow = operation.getRow();

        table.getSchema().getColumns().forEach(column -> {
            String columnName = column.getName();
            Object value = row.getField(column.getName());

            if (value == null) {
                partialRow.setNull(columnName);
            } else {
                Type type = column.getType();
                switch (type) {
                    case STRING:
                        partialRow.addString(columnName, (String) value);
                        break;
                    case FLOAT:
                        partialRow.addFloat(columnName, (Float) value);
                        break;
                    case INT8:
                        partialRow.addByte(columnName, (Byte) value);
                        break;
                    case INT16:
                        partialRow.addShort(columnName, (Short) value);
                        break;
                    case INT32:
                        partialRow.addInt(columnName, (Integer) value);
                        break;
                    case INT64:
                        partialRow.addLong(columnName, (Long) value);
                        break;
                    case DOUBLE:
                        partialRow.addDouble(columnName, (Double) value);
                        break;
                    case BOOL:
                        partialRow.addBoolean(columnName, (Boolean) value);
                        break;
                    case UNIXTIME_MICROS:
                        //*1000 to correctly create date on kudu
                        partialRow.addLong(columnName, ((Long) value) * 1000);
                        break;
                    case BINARY:
                        partialRow.addBinary(columnName, (byte[]) value);
                        break;
                    default:
                        throw new IllegalArgumentException("Illegal var type: " + type);
                }
            }
        });
        return operation;
    }

    private Operation obtainOperation() {
        switch (writerConfig.getWriteMode()) {
            case INSERT: return table.newInsert();
            case UPDATE: return table.newUpdate();
            case UPSERT: return table.newUpsert();
        }
        return table.newUpsert();
    }
}
