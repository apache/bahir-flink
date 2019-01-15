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

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kudu.connector.KuduConnector;
import org.apache.flink.streaming.connectors.kudu.connector.KuduRow;
import org.apache.flink.streaming.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.streaming.connectors.kudu.serde.KuduSerialization;
import org.apache.flink.util.Preconditions;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.RowError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class KuduSink<OUT> extends RichSinkFunction<OUT> {

    private static final Logger LOG = LoggerFactory.getLogger(KuduOutputFormat.class);

    private String kuduMasters;
    private KuduTableInfo tableInfo;
    private KuduConnector.Consistency consistency;
    private KuduConnector.WriteMode writeMode;

    private KuduSerialization<OUT> serializer;
    private Callback<Boolean, OperationResponse> callback;

    private transient KuduConnector tableContext;

    private static AtomicInteger pendingTransactions = new AtomicInteger();
    private static AtomicBoolean errorTransactions = new AtomicBoolean(false);
    private static Integer maxPendingTransactions = 100000;

    public KuduSink(String kuduMasters, KuduTableInfo tableInfo, KuduSerialization<OUT> serializer) {
        Preconditions.checkNotNull(kuduMasters,"kuduMasters could not be null");
        this.kuduMasters = kuduMasters;

        Preconditions.checkNotNull(tableInfo,"tableInfo could not be null");
        this.tableInfo = tableInfo;
        this.consistency = KuduConnector.Consistency.STRONG;
        this.writeMode = KuduConnector.WriteMode.UPSERT;
        this.serializer = serializer.withSchema(tableInfo.getSchema());
    }

    public KuduSink<OUT> withEventualConsistency() {
        this.consistency = KuduConnector.Consistency.EVENTUAL;
        return this;
    }

    public KuduSink<OUT> withStrongConsistency() {
        this.consistency = KuduConnector.Consistency.STRONG;
        return this;
    }

    public KuduSink<OUT> withUpsertWriteMode() {
        this.writeMode = KuduConnector.WriteMode.UPSERT;
        return this;
    }

    public KuduSink<OUT> withInsertWriteMode() {
        this.writeMode = KuduConnector.WriteMode.INSERT;
        return this;
    }

    public KuduSink<OUT> withUpdateWriteMode() {
        this.writeMode = KuduConnector.WriteMode.UPDATE;
        return this;
    }

    @Override
    public void open(Configuration parameters) throws IOException {
        startTableContext();
    }

    private void startTableContext() throws IOException {
        if (tableContext != null) return;
        tableContext = new KuduConnector(kuduMasters, tableInfo);
        serializer.withSchema(tableInfo.getSchema());
        callback = new ResponseCallback();
    }


    @Override
    public void invoke(OUT row) throws Exception {
        KuduRow kuduRow = serializer.serialize(row);
        Deferred<OperationResponse> response = tableContext.writeRow(kuduRow, writeMode);

        if (KuduConnector.Consistency.EVENTUAL.equals(consistency)) {
            pendingTransactions.incrementAndGet();
            response.addCallback(callback);
        } else {
            processResponse(response.join());
        }

        checkErrors();
    }

    private void checkErrors() throws Exception {
        if(errorTransactions.get()) {
            throw new IOException("error with some transaction");
        }
    }

    private class ResponseCallback implements Callback<Boolean, OperationResponse> {
        @Override
        public Boolean call(OperationResponse operationResponse) {
            pendingTransactions.decrementAndGet();
            return processResponse(operationResponse);
        }
    }

    private Boolean processResponse(OperationResponse operationResponse) {
        if (operationResponse == null) return true;

        if (operationResponse.hasRowError()) {
            logResponseError(operationResponse.getRowError());
            errorTransactions.set(true);
            return false;
        } else {
            return true;
        }
    }

    private void logResponseError(RowError error) {
        LOG.error("Error {} on {}: {} ", error.getErrorStatus(), error.getOperation(), error.toString());
    }


    @Override
    public void close() throws Exception {

        while(pendingTransactions.get() > 0) {
            LOG.info("sleeping {}s by pending transactions", pendingTransactions.get());
            Thread.sleep(Time.seconds(pendingTransactions.get()).toMilliseconds());
        }

        if (this.tableContext == null) return;
        try {
            this.tableContext.close();
        } catch (Exception e) {
            throw new IOException(e.getLocalizedMessage(), e);
        }
    }
}
