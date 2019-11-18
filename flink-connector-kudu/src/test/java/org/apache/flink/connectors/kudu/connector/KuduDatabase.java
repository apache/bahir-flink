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
package org.apache.flink.connectors.kudu.connector;

import org.apache.flink.connectors.kudu.connector.reader.KuduInputSplit;
import org.apache.flink.connectors.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.connectors.kudu.connector.reader.KuduReader;
import org.apache.flink.connectors.kudu.connector.reader.KuduReaderIterator;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriter;
import org.apache.kudu.Type;
import org.apache.kudu.test.KuduTestHarness;
import org.junit.Rule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.migrationsupport.rules.ExternalResourceSupport;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@ExtendWith(ExternalResourceSupport.class)
public class KuduDatabase {

    @Rule
    public static KuduTestHarness harness = new KuduTestHarness();

    private static final Object[][] booksTableData = {
            {1001, "Java for dummies", "Tan Ah Teck", 11.11, 11},
            {1002, "More Java for dummies", "Tan Ah Teck", 22.22, 22},
            {1003, "More Java for more dummies", "Mohammad Ali", 33.33, 33},
            {1004, "A Cup of Java", "Kumar", 44.44, 44},
            {1005, "A Teaspoon of Java", "Kevin Jones", 55.55, 55}};


    protected static KuduTableInfo booksTableInfo(String tableName, boolean createIfNotExist) {
        return KuduTableInfo.Builder
                .create(tableName)
                .createIfNotExist(createIfNotExist)
                .replicas(1)
                .addColumn(KuduColumnInfo.Builder.create("id", Type.INT32).key(true).hashKey(true).build())
                .addColumn(KuduColumnInfo.Builder.create("title", Type.STRING).build())
                .addColumn(KuduColumnInfo.Builder.create("author", Type.STRING).build())
                .addColumn(KuduColumnInfo.Builder.create("price", Type.DOUBLE).asNullable().build())
                .addColumn(KuduColumnInfo.Builder.create("quantity", Type.INT32).asNullable().build())
                .build();
    }

    protected static List<KuduRow> booksDataRow() {
        return Arrays.stream(booksTableData)
                .map(row -> {
                    Integer rowId = (Integer)row[0];
                    if (rowId % 2 == 1) {
                        KuduRow values = new KuduRow(5);
                        values.setField(0, "id", row[0]);
                        values.setField(1, "title", row[1]);
                        values.setField(2, "author", row[2]);
                        values.setField(3, "price", row[3]);
                        values.setField(4, "quantity", row[4]);
                        return values;
                    } else {
                        KuduRow values = new KuduRow(3);
                        values.setField(0, "id", row[0]);
                        values.setField(1, "title", row[1]);
                        values.setField(2, "author", row[2]);
                        return values;
                    }
                })
                .collect(Collectors.toList());
    }

    protected void setUpDatabase(KuduTableInfo tableInfo) {
        try {
            String masterAddresses = harness.getMasterAddressesAsString();
            KuduWriterConfig writerConfig = KuduWriterConfig.Builder.setMasters(masterAddresses).build();
            KuduWriter kuduWriter = new KuduWriter(tableInfo, writerConfig);
            booksDataRow().forEach(row -> {
                try {
                    kuduWriter.write(row);
                }catch (Exception e) {
                    e.printStackTrace();
                }
            });
            kuduWriter.close();
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    protected void cleanDatabase(KuduTableInfo tableInfo) {
        try {
            String masterAddresses = harness.getMasterAddressesAsString();
            KuduWriterConfig writerConfig = KuduWriterConfig.Builder.setMasters(masterAddresses).build();
            KuduWriter kuduWriter = new KuduWriter(tableInfo, writerConfig);
            kuduWriter.deleteTable();
            kuduWriter.close();
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    protected List<KuduRow> readRows(KuduTableInfo tableInfo) throws Exception {
        String masterAddresses = harness.getMasterAddressesAsString();
        KuduReaderConfig readerConfig = KuduReaderConfig.Builder.setMasters(masterAddresses).build();
        KuduReader reader = new KuduReader(tableInfo, readerConfig);

        KuduInputSplit[] splits = reader.createInputSplits(1);
        List<KuduRow> rows = new ArrayList<>();
        for (KuduInputSplit split : splits) {
            KuduReaderIterator resultIterator = reader.scanner(split.getScanToken());
            while(resultIterator.hasNext()) {
                KuduRow row = resultIterator.next();
                if(row != null) {
                    rows.add(row);
                }
            }
        }
        reader.close();

        return rows;
    }

    protected void kuduRowsTest(List<KuduRow> rows) {
        for (KuduRow row: rows) {
            Integer rowId = (Integer)row.getField("id");
            if (rowId % 2 == 1) {
                Assertions.assertNotEquals(null, row.getField("price"));
                Assertions.assertNotEquals(null, row.getField("quantity"));
            }
            else {
                Assertions.assertNull(row.getField("price"));
                Assertions.assertNull(row.getField("quantity"));
            }
        }
    }
}
