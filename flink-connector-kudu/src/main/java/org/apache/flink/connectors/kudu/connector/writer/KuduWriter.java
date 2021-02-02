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
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.failure.DefaultKuduFailureHandler;
import org.apache.flink.connectors.kudu.connector.failure.KuduFailureHandler;

import org.apache.kudu.client.DeleteTableResponse;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.RowError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@Internal
public class KuduWriter<T> implements AutoCloseable {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final KuduTableInfo tableInfo;
    private final KuduWriterConfig writerConfig;
    private final KuduFailureHandler failureHandler;
    private final KuduOperationMapper<T> operationMapper;

    private transient KuduClient client;
    private transient KuduSession session;
    private transient KuduTable table;

    public KuduWriter(KuduTableInfo tableInfo, KuduWriterConfig writerConfig, KuduOperationMapper<T> operationMapper) throws IOException {
        this(tableInfo, writerConfig, operationMapper, new DefaultKuduFailureHandler());
    }

    public KuduWriter(KuduTableInfo tableInfo, KuduWriterConfig writerConfig, KuduOperationMapper<T> operationMapper, KuduFailureHandler failureHandler) throws IOException {
        this.tableInfo = tableInfo;
        this.writerConfig = writerConfig;
        this.failureHandler = failureHandler;

        this.client = obtainClient();
        this.session = obtainSession();
        this.table = obtainTable();
        this.operationMapper = operationMapper;
    }

    private KuduClient obtainClient() {
        return new KuduClient.KuduClientBuilder(writerConfig.getMasters()).build();
    }

    private KuduSession obtainSession() {
        KuduSession session = client.newSession();
        session.setFlushMode(writerConfig.getFlushMode());
        session.setTimeoutMillis(writerConfig.getOperationTimeout());
        session.setMutationBufferSpace(writerConfig.getMaxBufferSize());
        session.setFlushInterval(writerConfig.getFlushInterval());
        session.setIgnoreAllDuplicateRows(writerConfig.isIgnoreDuplicate());
        session.setIgnoreAllNotFoundRows(writerConfig.isIgnoreNotFound());
        return session;
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

    public void write(T input) throws IOException {
        checkAsyncErrors();

        for (Operation operation : operationMapper.createOperations(input, table)) {
            checkErrors(session.apply(operation));
        }
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
        if (session.countPendingErrors() == 0) { return; }

        List<RowError> errors = Arrays.asList(session.getPendingErrors().getRowErrors());
        failureHandler.onFailure(errors);
    }
}
