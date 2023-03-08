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
package org.apache.flink.connectors.kudu.format;

import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.KuduTestBase;
import org.apache.flink.connectors.kudu.connector.writer.AbstractSingleOperationMapper;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.connectors.kudu.connector.writer.RowOperationMapper;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

class KuduOutputFormatTest extends KuduTestBase {

    @Test
    void testInvalidKuduMaster() {
        KuduTableInfo tableInfo = booksTableInfo(UUID.randomUUID().toString(), false);
        Assertions.assertThrows(NullPointerException.class, () -> new KuduOutputFormat<>(null, tableInfo, null));
    }

    @Test
    void testInvalidTableInfo() {
        String masterAddresses = getMasterAddress();
        KuduWriterConfig writerConfig = KuduWriterConfig.Builder.setMasters(masterAddresses).build();
        Assertions.assertThrows(NullPointerException.class, () -> new KuduOutputFormat<>(writerConfig, null, null));
    }

    @Test
    void testNotTableExist() {
        String masterAddresses = getMasterAddress();
        KuduTableInfo tableInfo = booksTableInfo(UUID.randomUUID().toString(), false);
        KuduWriterConfig writerConfig = KuduWriterConfig.Builder.setMasters(masterAddresses).build();
        KuduOutputFormat<Row> outputFormat = new KuduOutputFormat<>(writerConfig, tableInfo, new RowOperationMapper(KuduTestBase.columns, AbstractSingleOperationMapper.KuduOperation.INSERT));
        Assertions.assertThrows(RuntimeException.class, () -> outputFormat.open(0, 1));
    }

    @Test
    void testOutputWithStrongConsistency() throws Exception {
        String masterAddresses = getMasterAddress();

        KuduTableInfo tableInfo = booksTableInfo(UUID.randomUUID().toString(), true);
        KuduWriterConfig writerConfig = KuduWriterConfig.Builder
                .setMasters(masterAddresses)
                .setStrongConsistency()
                .build();
        KuduOutputFormat<Row> outputFormat = new KuduOutputFormat<>(writerConfig, tableInfo, new RowOperationMapper(KuduTestBase.columns, AbstractSingleOperationMapper.KuduOperation.INSERT));

        outputFormat.open(0, 1);

        for (Row kuduRow : booksDataRow()) {
            outputFormat.writeRecord(kuduRow);
        }
        outputFormat.close();

        List<Row> rows = readRows(tableInfo);
        Assertions.assertEquals(5, rows.size());
        kuduRowsTest(rows);

        cleanDatabase(tableInfo);
    }

    @Test
    void testOutputWithEventualConsistency() throws Exception {
        String masterAddresses = getMasterAddress();

        KuduTableInfo tableInfo = booksTableInfo(UUID.randomUUID().toString(), true);
        KuduWriterConfig writerConfig = KuduWriterConfig.Builder
                .setMasters(masterAddresses)
                .setEventualConsistency()
                .build();
        KuduOutputFormat<Row> outputFormat = new KuduOutputFormat<>(writerConfig, tableInfo, new RowOperationMapper(KuduTestBase.columns, AbstractSingleOperationMapper.KuduOperation.INSERT));

        outputFormat.open(0, 1);

        for (Row kuduRow : booksDataRow()) {
            outputFormat.writeRecord(kuduRow);
        }

        // sleep to allow eventual consistency to finish
        Thread.sleep(1000);

        outputFormat.close();

        List<Row> rows = readRows(tableInfo);
        Assertions.assertEquals(5, rows.size());
        kuduRowsTest(rows);

        cleanDatabase(tableInfo);
    }

}
