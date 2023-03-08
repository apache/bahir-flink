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
package org.apache.flink.connectors.kudu.streaming;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.KuduTestBase;
import org.apache.flink.connectors.kudu.connector.writer.AbstractSingleOperationMapper;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.connectors.kudu.connector.writer.RowOperationMapper;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.types.Row;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.shaded.com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

public class KuduSinkTest extends KuduTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(KuduSinkTest.class);
    private static final String[] columns = new String[]{"id", "uuid"};
    private static StreamingRuntimeContext context;

    @BeforeAll
    static void start() {
        context = Mockito.mock(StreamingRuntimeContext.class);
        Mockito.when(context.isCheckpointingEnabled()).thenReturn(true);
    }

    @Test
    void testInvalidKuduMaster() {
        KuduTableInfo tableInfo = booksTableInfo(UUID.randomUUID().toString(), false);
        Assertions.assertThrows(NullPointerException.class, () -> new KuduSink<>(null, tableInfo, new RowOperationMapper(columns, AbstractSingleOperationMapper.KuduOperation.INSERT)));
    }

    @Test
    void testInvalidTableInfo() {
        String masterAddresses = getMasterAddress();
        KuduWriterConfig writerConfig = KuduWriterConfig.Builder.setMasters(masterAddresses).build();
        Assertions.assertThrows(NullPointerException.class, () -> new KuduSink<>(writerConfig, null, new RowOperationMapper(columns, AbstractSingleOperationMapper.KuduOperation.INSERT)));
    }

    @Test
    void testNotTableExist() {
        String masterAddresses = getMasterAddress();
        KuduTableInfo tableInfo = booksTableInfo(UUID.randomUUID().toString(), false);
        KuduWriterConfig writerConfig = KuduWriterConfig.Builder.setMasters(masterAddresses).build();
        KuduSink<Row> sink = new KuduSink<>(writerConfig, tableInfo, new RowOperationMapper(columns, AbstractSingleOperationMapper.KuduOperation.INSERT));

        sink.setRuntimeContext(context);
        Assertions.assertThrows(RuntimeException.class, () -> sink.open(new Configuration()));
    }

    @Test
    void testOutputWithStrongConsistency() throws Exception {
        String masterAddresses = getMasterAddress();

        KuduTableInfo tableInfo = booksTableInfo(UUID.randomUUID().toString(), true);
        KuduWriterConfig writerConfig = KuduWriterConfig.Builder
                .setMasters(masterAddresses)
                .setStrongConsistency()
                .build();
        KuduSink<Row> sink = new KuduSink<>(writerConfig, tableInfo, new RowOperationMapper(KuduTestBase.columns, AbstractSingleOperationMapper.KuduOperation.INSERT));

        sink.setRuntimeContext(context);
        sink.open(new Configuration());

        for (Row kuduRow : booksDataRow()) {
            sink.invoke(kuduRow);
        }
        sink.close();

        List<Row> rows = readRows(tableInfo);
        Assertions.assertEquals(5, rows.size());
        kuduRowsTest(rows);
    }

    @Test
    void testOutputWithEventualConsistency() throws Exception {
        String masterAddresses = getMasterAddress();

        KuduTableInfo tableInfo = booksTableInfo(UUID.randomUUID().toString(), true);
        KuduWriterConfig writerConfig = KuduWriterConfig.Builder
                .setMasters(masterAddresses)
                .setEventualConsistency()
                .build();
        KuduSink<Row> sink = new KuduSink<>(writerConfig, tableInfo, new RowOperationMapper(KuduTestBase.columns, AbstractSingleOperationMapper.KuduOperation.INSERT));

        sink.setRuntimeContext(context);
        sink.open(new Configuration());

        for (Row kuduRow : booksDataRow()) {
            sink.invoke(kuduRow);
        }

        // sleep to allow eventual consistency to finish
        Thread.sleep(1000);

        sink.close();

        List<Row> rows = readRows(tableInfo);
        Assertions.assertEquals(5, rows.size());
        kuduRowsTest(rows);
    }

    @Test
    void testSpeed() throws Exception {
        String masterAddresses = getMasterAddress();

        KuduTableInfo tableInfo = KuduTableInfo
                .forTable("test_speed")
                .createTableIfNotExists(
                        () ->
                                Lists.newArrayList(
                                        new ColumnSchema
                                                .ColumnSchemaBuilder("id", Type.INT32)
                                                .key(true)
                                                .build(),
                                        new ColumnSchema
                                                .ColumnSchemaBuilder("uuid", Type.STRING)
                                                .build()
                                ),
                        () -> new CreateTableOptions()
                                .setNumReplicas(3)
                                .addHashPartitions(Lists.newArrayList("id"), 6));

        KuduWriterConfig writerConfig = KuduWriterConfig.Builder
                .setMasters(masterAddresses)
                .setEventualConsistency()
                .build();
        KuduSink<Row> sink = new KuduSink<>(writerConfig, tableInfo, new RowOperationMapper(columns, AbstractSingleOperationMapper.KuduOperation.INSERT));

        sink.setRuntimeContext(context);
        sink.open(new Configuration());

        int totalRecords = 100000;
        for (int i = 0; i < totalRecords; i++) {
            Row kuduRow = new Row(2);
            kuduRow.setField(0, i);
            kuduRow.setField(1, UUID.randomUUID().toString());
            sink.invoke(kuduRow);
        }

        // sleep to allow eventual consistency to finish
        Thread.sleep(1000);

        sink.close();

        List<Row> rows = readRows(tableInfo);
        Assertions.assertEquals(totalRecords, rows.size());
    }

}
