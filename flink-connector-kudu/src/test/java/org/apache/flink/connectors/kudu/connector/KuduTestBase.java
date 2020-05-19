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

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connectors.kudu.connector.reader.KuduInputSplit;
import org.apache.flink.connectors.kudu.connector.reader.KuduReader;
import org.apache.flink.connectors.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connectors.kudu.connector.reader.KuduReaderIterator;
import org.apache.flink.connectors.kudu.connector.writer.AbstractSingleOperationMapper;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriter;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.connectors.kudu.connector.writer.RowOperationMapper;
import org.apache.flink.types.Row;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.shaded.com.google.common.collect.Lists;
import org.apache.kudu.test.KuduTestHarness;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.migrationsupport.rules.ExternalResourceSupport;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(ExternalResourceSupport.class)
public class KuduTestBase {

    private static final Object[][] booksTableData = {
            {1001, "Java for dummies", "Tan Ah Teck", 11.11, 11},
            {1002, "More Java for dummies", "Tan Ah Teck", 22.22, 22},
            {1003, "More Java for more dummies", "Mohammad Ali", 33.33, 33},
            {1004, "A Cup of Java", "Kumar", 44.44, 44},
            {1005, "A Teaspoon of Java", "Kevin Jones", 55.55, 55}};

    public static KuduTestHarness harness;
    public static String[] columns = new String[]{"id", "title", "author", "price", "quantity"};

    @BeforeAll
    public static void beforeClass() throws Exception {
        harness = new KuduTestHarness();
        harness.before();
    }

    @AfterAll
    public static void afterClass() throws Exception {
        harness.after();
    }

    public static KuduTableInfo booksTableInfo(String tableName, boolean createIfNotExist) {

        KuduTableInfo tableInfo = KuduTableInfo.forTable(tableName);

        if (createIfNotExist) {
            ColumnSchemasFactory schemasFactory = () -> {
                List<ColumnSchema> schemas = new ArrayList<>();
                schemas.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).build());
                schemas.add(new ColumnSchema.ColumnSchemaBuilder("title", Type.STRING).build());
                schemas.add(new ColumnSchema.ColumnSchemaBuilder("author", Type.STRING).build());
                schemas.add(new ColumnSchema.ColumnSchemaBuilder("price", Type.DOUBLE).nullable(true).build());
                schemas.add(new ColumnSchema.ColumnSchemaBuilder("quantity", Type.INT32).nullable(true).build());
                return schemas;
            };

            tableInfo.createTableIfNotExists(
                    schemasFactory,
                    () -> new CreateTableOptions()
                            .setNumReplicas(1)
                            .addHashPartitions(Lists.newArrayList("id"), 2));
        }

        return tableInfo;
    }

    public static List<Tuple5<Integer, String, String, Double, Integer>> booksDataTuple() {
        return Arrays.stream(booksTableData)
                .map(row -> {
                    Integer rowId = (Integer) row[0];
                    if (rowId % 2 == 1) {
                        Tuple5<Integer, String, String, Double, Integer> values =
                                Tuple5.of((Integer) row[0],
                                        (String) row[1],
                                        (String) row[2],
                                        (Double) row[3],
                                        (Integer)row[4]);
                        return values;
                    } else {
                        Tuple5<Integer, String, String, Double, Integer> values =
                                Tuple5.of((Integer) row[0],
                                        (String) row[1],
                                        (String) row[2],
                                        null, null);
                        return values;
                    }
                })
                .collect(Collectors.toList());
    }

    public static List<Row> booksDataRow() {
        return Arrays.stream(booksTableData)
                .map(row -> {
                    Integer rowId = (Integer) row[0];
                    if (rowId % 2 == 1) {
                        Row values = new Row(5);
                        values.setField(0, row[0]);
                        values.setField(1, row[1]);
                        values.setField(2, row[2]);
                        values.setField(3, row[3]);
                        values.setField(4, row[4]);
                        return values;
                    } else {
                        Row values = new Row(5);
                        values.setField(0, row[0]);
                        values.setField(1, row[1]);
                        values.setField(2, row[2]);
                        return values;
                    }
                })
                .collect(Collectors.toList());
    }

    public static List<BookInfo> booksDataPojo() {
        return Arrays.stream(booksTableData).map(row -> new BookInfo(
                (int) row[0],
                (String) row[1],
                (String) row[2],
                (Double) row[3],
                (int) row[4]))
                .collect(Collectors.toList());
    }

    protected void setUpDatabase(KuduTableInfo tableInfo) {
        try {
            String masterAddresses = harness.getMasterAddressesAsString();
            KuduWriterConfig writerConfig = KuduWriterConfig.Builder.setMasters(masterAddresses).build();
            KuduWriter kuduWriter = new KuduWriter(tableInfo, writerConfig, new RowOperationMapper(columns, AbstractSingleOperationMapper.KuduOperation.INSERT));
            booksDataRow().forEach(row -> {
                try {
                    kuduWriter.write(row);
                } catch (Exception e) {
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
            KuduWriter kuduWriter = new KuduWriter(tableInfo, writerConfig, new RowOperationMapper(columns, AbstractSingleOperationMapper.KuduOperation.INSERT));
            kuduWriter.deleteTable();
            kuduWriter.close();
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    protected List<Row> readRows(KuduTableInfo tableInfo) throws Exception {
        String masterAddresses = harness.getMasterAddressesAsString();
        KuduReaderConfig readerConfig = KuduReaderConfig.Builder.setMasters(masterAddresses).build();
        KuduReader reader = new KuduReader(tableInfo, readerConfig);

        KuduInputSplit[] splits = reader.createInputSplits(1);
        List<Row> rows = new ArrayList<>();
        for (KuduInputSplit split : splits) {
            KuduReaderIterator resultIterator = reader.scanner(split.getScanToken());
            while (resultIterator.hasNext()) {
                Row row = resultIterator.next();
                if (row != null) {
                    rows.add(row);
                }
            }
        }
        reader.close();

        return rows;
    }

    protected void kuduRowsTest(List<Row> rows) {
        for (Row row : rows) {
            Integer rowId = (Integer) row.getField(0);
            if (rowId % 2 == 1) {
                Assertions.assertNotEquals(null, row.getField(3));
                Assertions.assertNotEquals(null, row.getField(4));
            } else {
                Assertions.assertNull(row.getField(3));
                Assertions.assertNull(row.getField(4));
            }
        }
    }

    protected void validateSingleKey(String tableName) throws Exception {
        KuduTable kuduTable = harness.getClient().openTable(tableName);
        Schema schema = kuduTable.getSchema();

        assertEquals(1, schema.getPrimaryKeyColumnCount());
        assertEquals(2, schema.getColumnCount());

        assertTrue(schema.getColumn("first").isKey());
        assertFalse(schema.getColumn("second").isKey());

        KuduScanner scanner = harness.getClient().newScannerBuilder(kuduTable).build();
        List<RowResult> rows = new ArrayList<>();
        scanner.forEach(rows::add);

        assertEquals(1, rows.size());
        assertEquals("f", rows.get(0).getString("first"));
        assertEquals("s", rows.get(0).getString("second"));
    }

    public static class BookInfo {
        public int id, quantity;
        public String title, author;
        public Double price;

        public BookInfo() {
        }

        public BookInfo(int id, String title, String author, Double price, int quantity) {
            this.id = id;
            this.title = title;
            this.author = author;
            this.price = price;
            this.quantity = quantity;
        }
    }
}
