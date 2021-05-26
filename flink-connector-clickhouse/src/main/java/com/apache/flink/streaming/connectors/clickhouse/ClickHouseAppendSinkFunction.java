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
package com.apache.flink.streaming.connectors.clickhouse;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Properties;

public class ClickHouseAppendSinkFunction extends RichSinkFunction<Row> implements CheckpointedFunction {
    private static final String USERNAME = "user";
    private static final String PASSWORD = "password";

    private static final Logger log = LoggerFactory.getLogger(ClickHouseAppendSinkFunction.class);
    private static final long serialVersionUID = 1L;

    private  Connection connection;
    private  BalancedClickhouseDataSource dataSource;
    private  PreparedStatement pstat;

    private String address;
    private String username;
    private String password;

    private String prepareStatement;
    private Integer batchSize;
    private Long commitPadding;

    private Integer retries;
    private Long retryInterval;

    private Boolean ignoreInsertError;

    private Integer currentSize;
    private Long lastExecuteTime;

    public ClickHouseAppendSinkFunction(String address, String username, String password, String prepareStatement, Integer batchSize, Long commitPadding, Integer retries, Long retryInterval, Boolean ignoreInsertError) {
        this.address = address;
        this.username = username;
        this.password = password;
        this.prepareStatement = prepareStatement;
        this.batchSize = batchSize;
        this.commitPadding = commitPadding;
        this.retries = retries;
        this.retryInterval = retryInterval;
        this.ignoreInsertError = ignoreInsertError;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Properties properties = new Properties();
        properties.setProperty(USERNAME, username);
        properties.setProperty(PASSWORD, password);
        ClickHouseProperties clickHouseProperties = new ClickHouseProperties(properties);
        dataSource = new BalancedClickhouseDataSource(address, clickHouseProperties);
        connection = dataSource.getConnection();
        pstat = connection.prepareStatement(prepareStatement);
        lastExecuteTime = System.currentTimeMillis();
        currentSize = 0;

    }

    @Override
    public void invoke(Row value, Context context) throws Exception {
        for (int i = 0; i < value.getArity(); i++) {
            pstat.setObject(i + 1, value.getField(i));
        }
        pstat.addBatch();
        currentSize++;
        if (currentSize >= batchSize || (System.currentTimeMillis() - lastExecuteTime) > commitPadding) {
            try {
                doExecuteRetries(retries, retryInterval);
            } catch (Exception e) {
                log.error("clickhouse-insert-error ( maxRetries:" + retries + " , retryInterval : " + retryInterval + " millisecond )" + e.getMessage());
            } finally {
                pstat.clearBatch();
                currentSize = 0;
                lastExecuteTime = System.currentTimeMillis();
            }
        }
    }

    public void doExecuteRetries(int count, long retryInterval) throws Exception {

        int retrySize = 0;
        Exception resultException = null;
        for (int i = 0; i < count; i++) {
            try {
                pstat.executeBatch();
                break;
            } catch (Exception e) {
                retrySize++;
                resultException = e;
            }
            try {
                Thread.sleep(retryInterval);
            } catch (InterruptedException e) {
                log.error("clickhouse retry interval exception : ",e);
            }
        }
        if (retrySize == count && !ignoreInsertError) {
            throw resultException;
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        doExecuteRetries(retries, retryInterval);
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }

}
