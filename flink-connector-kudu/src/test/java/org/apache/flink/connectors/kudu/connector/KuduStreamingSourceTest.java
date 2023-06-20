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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.kudu.connector.convertor.RowResultRowConvertor;
import org.apache.flink.connectors.kudu.connector.reader.KuduInputSplit;
import org.apache.flink.connectors.kudu.connector.reader.KuduReader;
import org.apache.flink.connectors.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connectors.kudu.connector.reader.KuduReaderIterator;
import org.apache.flink.connectors.kudu.connector.writer.AbstractSingleOperationMapper;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.connectors.kudu.connector.writer.RowOperationMapper;
import org.apache.flink.connectors.kudu.streaming.KuduSink;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.types.Row;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.shaded.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

public class KuduStreamingSourceTest {
    private static final String[] columns = new String[]{"name_col", "id_col", "age_col"};

    private static StreamingRuntimeContext context;

    private static final String masterAddresses = "192.168.0.68:7051,192.168.0.68:7151,192.168.0.68:7251";

    @BeforeAll
    static void start() {
        context = Mockito.mock(StreamingRuntimeContext.class);
        Mockito.when(context.isCheckpointingEnabled()).thenReturn(true);
    }

    private void prepareData() throws Exception {
        KuduTableInfo tableInfo = KuduTableInfo
                .forTable("users")
                .createTableIfNotExists(
                        () ->
                                Lists.newArrayList(
                                        new ColumnSchema
                                                .ColumnSchemaBuilder("name_col", Type.STRING)
                                                .key(true)
                                                .build(),
                                        new ColumnSchema
                                                .ColumnSchemaBuilder("id_col", Type.INT64)
                                                .key(true)
                                                .build(),
                                        new ColumnSchema
                                                .ColumnSchemaBuilder("age_col", Type.INT32)
                                                .key(false)
                                                .build()
                                ),
                        () -> new CreateTableOptions()
                                .setNumReplicas(3)
                                .addHashPartitions(Lists.newArrayList("name_col", "id_col"), 6));

        KuduWriterConfig writerConfig = KuduWriterConfig.Builder
                .setMasters(masterAddresses)
                .setEventualConsistency()
                .build();
        KuduSink<Row> sink = new KuduSink<>(writerConfig, tableInfo, new RowOperationMapper(columns, AbstractSingleOperationMapper.KuduOperation.INSERT));

        sink.setRuntimeContext(context);
        sink.open(new Configuration());


        Row kuduRow = new Row(3);
        kuduRow.setField(0, "Tom");
        kuduRow.setField(1, 1000L);
        kuduRow.setField(2, 10);

        Row kuduRow2 = new Row(3);
        kuduRow2.setField(0, "Mary");
        kuduRow2.setField(1, 2000L);
        kuduRow2.setField(2, 20);

        Row kuduRow3 = new Row(3);
        kuduRow3.setField(0, "Jack");
        kuduRow3.setField(1, 3000L);
        kuduRow3.setField(2, 30);

        sink.invoke(kuduRow);
        sink.invoke(kuduRow2);
        sink.invoke(kuduRow3);


        // sleep to allow eventual consistency to finish
        Thread.sleep(1000);

        sink.close();

        List<Row> rows = readRows(tableInfo);
        for (Row row : rows) {
            System.out.println(row.getField(0) + " " + row.getField(1) + " " + row.getField(2));
        }
    }

    protected List<Row> readRows(KuduTableInfo tableInfo) throws Exception {
        KuduReaderConfig readerConfig = KuduReaderConfig.Builder.setMasters(masterAddresses).build();
        KuduReader<Row> reader = new KuduReader<>(tableInfo, readerConfig, new RowResultRowConvertor());

        KuduInputSplit[] splits = reader.createInputSplits(1);
        List<Row> rows = new ArrayList<>();
        for (KuduInputSplit split : splits) {
            KuduReaderIterator<Row> resultIterator = reader.scanner(split.getScanToken());
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
    @Test
    void testCustomQuery() throws Exception {
        prepareData();
    }
}
