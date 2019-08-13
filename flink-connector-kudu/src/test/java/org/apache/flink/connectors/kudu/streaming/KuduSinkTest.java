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
import org.apache.flink.connectors.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.connectors.kudu.connector.KuduColumnInfo;
import org.apache.flink.connectors.kudu.connector.KuduDatabase;
import org.apache.flink.connectors.kudu.connector.KuduRow;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.serde.DefaultSerDe;
import org.apache.kudu.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.UUID;

class KuduSinkTest extends KuduDatabase {

    private static StreamingRuntimeContext context;

    @BeforeAll
    static void start() {
        context = Mockito.mock(StreamingRuntimeContext.class);
        Mockito.when(context.isCheckpointingEnabled()).thenReturn(true);
    }

    @Test
    void testInvalidKuduMaster() {
        KuduTableInfo tableInfo = booksTableInfo(UUID.randomUUID().toString(),false);
        Assertions.assertThrows(NullPointerException.class, () -> new KuduSink<>(null, tableInfo, new DefaultSerDe()));
    }

    @Test
    void testInvalidTableInfo() {
        String masterAddresses = harness.getMasterAddressesAsString();
        KuduWriterConfig writerConfig = KuduWriterConfig.Builder.setMasters(masterAddresses).build();
        Assertions.assertThrows(NullPointerException.class, () -> new KuduSink<>(writerConfig, null, new DefaultSerDe()));
    }

    @Test
    void testNotTableExist() {
        String masterAddresses = harness.getMasterAddressesAsString();
        KuduTableInfo tableInfo = booksTableInfo(UUID.randomUUID().toString(),false);
        KuduWriterConfig writerConfig = KuduWriterConfig.Builder.setMasters(masterAddresses).build();
        KuduSink<KuduRow> sink = new KuduSink<>(writerConfig, tableInfo, new DefaultSerDe());

        sink.setRuntimeContext(context);
        Assertions.assertThrows(UnsupportedOperationException.class, () -> sink.open(new Configuration()));
    }

    @Test
    void testOutputWithStrongConsistency() throws Exception {
        String masterAddresses = harness.getMasterAddressesAsString();

        KuduTableInfo tableInfo = booksTableInfo(UUID.randomUUID().toString(),true);
        KuduWriterConfig writerConfig = KuduWriterConfig.Builder
                .setMasters(masterAddresses)
                .setStrongConsistency()
                .build();
        KuduSink<KuduRow> sink = new KuduSink<>(writerConfig, tableInfo, new DefaultSerDe());

        sink.setRuntimeContext(context);
        sink.open(new Configuration());

        for (KuduRow kuduRow : booksDataRow()) {
            sink.invoke(kuduRow);
        }
        sink.close();

        List<KuduRow> rows = readRows(tableInfo);
        Assertions.assertEquals(5, rows.size());

    }

    @Test
    void testOutputWithEventualConsistency() throws Exception {
        String masterAddresses = harness.getMasterAddressesAsString();

        KuduTableInfo tableInfo = booksTableInfo(UUID.randomUUID().toString(),true);
        KuduWriterConfig writerConfig = KuduWriterConfig.Builder
                .setMasters(masterAddresses)
                .setEventualConsistency()
                .build();
        KuduSink<KuduRow> sink = new KuduSink<>(writerConfig, tableInfo, new DefaultSerDe());

        sink.setRuntimeContext(context);
        sink.open(new Configuration());

        for (KuduRow kuduRow : booksDataRow()) {
            sink.invoke(kuduRow);
        }

        // sleep to allow eventual consistency to finish
        Thread.sleep(1000);

        sink.close();

        List<KuduRow> rows = readRows(tableInfo);
        Assertions.assertEquals(5, rows.size());
    }


    @Test
    void testSpeed() throws Exception {
        String masterAddresses = harness.getMasterAddressesAsString();

        KuduTableInfo tableInfo = KuduTableInfo.Builder
                .create("test_speed")
                .createIfNotExist(true)
                .replicas(3)
                .addColumn(KuduColumnInfo.Builder.create("id", Type.INT32).key(true).hashKey(true).build())
                .addColumn(KuduColumnInfo.Builder.create("uuid", Type.STRING).build())
                .build();
        KuduWriterConfig writerConfig = KuduWriterConfig.Builder
                .setMasters(masterAddresses)
                .setEventualConsistency()
                .build();
        KuduSink<KuduRow> sink = new KuduSink<>(writerConfig, tableInfo, new DefaultSerDe());

        sink.setRuntimeContext(context);
        sink.open(new Configuration());

        int totalRecords = 100000;
        for (int i=0; i < totalRecords; i++) {
            KuduRow kuduRow = new KuduRow(2);
            kuduRow.setField(0, "id", i);
            kuduRow.setField(1, "uuid", UUID.randomUUID().toString());
            sink.invoke(kuduRow);
        }

        // sleep to allow eventual consistency to finish
        Thread.sleep(1000);

        sink.close();

        List<KuduRow> rows = readRows(tableInfo);
        Assertions.assertEquals(totalRecords, rows.size());
    }

}
