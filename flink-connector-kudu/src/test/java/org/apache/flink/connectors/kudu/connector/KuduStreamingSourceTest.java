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

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.kudu.connector.configuration.UserTableDataQueryDetail;
import org.apache.flink.connectors.kudu.connector.configuration.type.FilterOp;
import org.apache.flink.connectors.kudu.connector.configuration.type.UserTableDataQueryFilter;
import org.apache.flink.connectors.kudu.connector.configuration.type.annotation.ColumnDetail;
import org.apache.flink.connectors.kudu.connector.configuration.type.annotation.StreamingKey;
import org.apache.flink.connectors.kudu.connector.writer.AbstractSingleOperationMapper;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.connectors.kudu.connector.writer.RowOperationMapper;
import org.apache.flink.connectors.kudu.streaming.KuduSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.types.Row;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.shaded.com.google.common.collect.Lists;
import org.apache.kudu.shaded.com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.*;

public class KuduStreamingSourceTest extends KuduTestBase {

    private static StreamingRuntimeContext context;

    @BeforeAll
    public static void start() {
        context = Mockito.mock(StreamingRuntimeContext.class);
        Mockito.when(context.isCheckpointingEnabled()).thenReturn(true);
    }

    private void prepareData() throws Exception {
        String masterAddresses = getMasterAddress();
        String[] columns = new String[]{"name_col", "id_col", "age_col"};
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
                                .addHashPartitions(Lists.newArrayList("name_col", "id_col"), 3));

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
    }

    @Test
    void testCustomQuery() throws Exception {
        String masterAddresses = getMasterAddress();

        prepareData();

        UserTableDataQueryDetail detail = new UserTableDataQueryDetail();
        detail.setProjectedColumns(Arrays.asList("name_col", "id_col", "age_col"));

        UserTableDataQueryFilter nameFilter = UserTableDataQueryFilter.builder()
                .colName("name_col")
                .filterOp(FilterOp.GREATER_EQUAL)
                .filterValueResolver(() -> "Jack").build();

        UserTableDataQueryFilter idFilter = UserTableDataQueryFilter.builder()
                .colName("id_col")
                .filterOp(FilterOp.LESS_EQUAL)
                .filterValueResolver(() -> 2000L).build();
        detail.setUserTableDataQueryFilters(Arrays.asList(nameFilter, idFilter));

        KuduStreamingSourceConfiguration<UserType> configuration =
                KuduStreamingSourceConfiguration.<UserType>builder()
                        .masterAddresses(masterAddresses)
                        .tableName("users")
                        .batchRunningInterval(1000L)
                        .runningMode(KuduStreamingRunningMode.CUSTOM_QUERY)
                        .targetKuduRowClz(UserType.class)
                        .userTableDataQueryDetailList(Arrays.asList(detail))
                        .build();

        KuduStreamingSource<UserType> sourceFunction = new KuduStreamingSource<>(configuration);

        EchoSink echoSink = new EchoSink();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(sourceFunction).setParallelism(1)
                .returns(TypeInformation.of(new TypeHint<UserType>() {}))
                .addSink(echoSink).setParallelism(1);


        CompletableFuture<Void> completableFuture = CompletableFuture.runAsync(() -> {
           try {
               env.execute();
           } catch (Exception e) {}
        });

        try {
            completableFuture.get(3, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            completableFuture.cancel(true);
        }

        int matched = 0;
        for (UserType user : echoSink.getCollectedData()) {
            if (user.equals(new UserType(1000L, "Tom", 10))) {
                matched++;
            } else if (user.equals(new UserType(2000L, "Mary", 20))) {
                matched++;
            }
        }

        Assertions.assertEquals(2, matched);
    }

    @Test
    public void testIncremental() throws Exception {
        String masterAddresses = getMasterAddress();
        String tableName = "streaming_key_types";
        String[] columns = {"long_val", "int_val", "short_val", "byte_val", "str_val"};
        KuduTableInfo tableInfo = KuduTableInfo
                .forTable(tableName)
                .createTableIfNotExists(
                        () ->
                                Lists.newArrayList(
                                        new ColumnSchema
                                                .ColumnSchemaBuilder("long_val", Type.INT64)
                                                .key(true)
                                                .build(),
                                        new ColumnSchema
                                                .ColumnSchemaBuilder("int_val", Type.INT32)
                                                .key(true)
                                                .build(),
                                        new ColumnSchema
                                                .ColumnSchemaBuilder("short_val", Type.INT16)
                                                .key(true)
                                                .build(),
                                        new ColumnSchema
                                                .ColumnSchemaBuilder("byte_val", Type.INT8)
                                                .key(true)
                                                .build(),
                                        new ColumnSchema
                                                .ColumnSchemaBuilder("str_val", Type.STRING)
                                                .key(true)
                                                .build()
                                ),
                        () -> new CreateTableOptions()
                                .setNumReplicas(3)
                                .addHashPartitions(
                                        Lists.newArrayList(columns),
                                        3));

        KuduWriterConfig writerConfig = KuduWriterConfig.Builder
                .setMasters(masterAddresses)
                .setEventualConsistency()
                .build();
        KuduSink<Row> sink = new KuduSink<>(writerConfig, tableInfo, new RowOperationMapper(columns, AbstractSingleOperationMapper.KuduOperation.INSERT));

        sink.setRuntimeContext(context);
        sink.open(new Configuration());

        Row kuduRow = new Row(5);
        kuduRow.setField(0, 1L);
        kuduRow.setField(1, 1);
        kuduRow.setField(2, (short)1);
        kuduRow.setField(3, (byte)1);
        kuduRow.setField(4, "a");

        Row kuduRow2 = new Row(5);
        kuduRow2.setField(0, 1L);
        kuduRow2.setField(1, 2);
        kuduRow2.setField(2, (short)1);
        kuduRow2.setField(3, (byte)1);
        kuduRow2.setField(4, "a");

        Row kuduRow3 = new Row(5);
        kuduRow3.setField(0, 1L);
        kuduRow3.setField(1, 2);
        kuduRow3.setField(2, (short)3);
        kuduRow3.setField(3, (byte)1);
        kuduRow3.setField(4, "a");

        Row kuduRow4 = new Row(5);
        kuduRow4.setField(0, 1L);
        kuduRow4.setField(1, 2);
        kuduRow4.setField(2, (short)3);
        kuduRow4.setField(3, (byte)4);
        kuduRow4.setField(4, "a");

        Row kuduRow5 = new Row(5);
        kuduRow5.setField(0, 1L);
        kuduRow5.setField(1, 2);
        kuduRow5.setField(2, (short)3);
        kuduRow5.setField(3, (byte)4);
        kuduRow5.setField(4, "b");

        sink.invoke(kuduRow);
        sink.invoke(kuduRow2);
        sink.invoke(kuduRow3);
        sink.invoke(kuduRow4);
        sink.invoke(kuduRow5);

        Thread.sleep(1000);

        sink.close();

        UserTableDataQueryDetail detail = new UserTableDataQueryDetail();
        detail.setProjectedColumns(Arrays.asList(columns));

        KuduStreamingSourceConfiguration<StreamingKeyType> configuration =
                KuduStreamingSourceConfiguration.<StreamingKeyType>builder()
                        .masterAddresses(masterAddresses)
                        .tableName(tableName)
                        .batchRunningInterval(1000L)
                        .runningMode(KuduStreamingRunningMode.INCREMENTAL)
                        .targetKuduRowClz(StreamingKeyType.class)
                        .userTableDataQueryDetailList(Arrays.asList(detail))
                        .build();

        KuduStreamingSource<StreamingKeyType> sourceFunction = new KuduStreamingSource<>(configuration);

        OrderedEchoSink orderedEchoSink = new OrderedEchoSink();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);
        env.addSource(sourceFunction).setParallelism(1)
                .returns(TypeInformation.of(new TypeHint<StreamingKeyType>() {}))
                .addSink(orderedEchoSink).setParallelism(1);



        CompletableFuture<Void> completableFuture = CompletableFuture.runAsync(() -> {
            try {
                env.execute();
            } catch (Exception e) {}
        });

        try {
            completableFuture.get(10, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            completableFuture.cancel(true);
        }

        List<StreamingKeyType> typeList = orderedEchoSink.getOrderedData();
        Assertions.assertEquals(new StreamingKeyType(1L, 1, (short)1, (byte)1, "a"), typeList.get(0));
        Assertions.assertEquals(new StreamingKeyType(1L, 2, (short)1, (byte)1, "a"), typeList.get(1));
        Assertions.assertEquals(new StreamingKeyType(1L, 2, (short)3, (byte)1, "a"), typeList.get(2));
        Assertions.assertEquals(new StreamingKeyType(1L, 2, (short)3, (byte)4, "a"), typeList.get(3));
        Assertions.assertEquals(new StreamingKeyType(1L, 2, (short)3, (byte)4, "b"), typeList.get(4));
    }

    public static class StreamingKeyType {
        @StreamingKey(order = 1)
        @ColumnDetail(name = "long_val")
        private Long longVal;

        @StreamingKey(order = 2)
        @ColumnDetail(name = "int_val")
        private Integer intVal;

        @StreamingKey(order = 3)
        @ColumnDetail(name = "short_val")
        private Short shortVal;

        @StreamingKey(order = 4)
        @ColumnDetail(name = "byte_val")
        private Byte byteVal;

        @StreamingKey(order = 5)
        @ColumnDetail(name = "str_val")
        private String strVal;

        public StreamingKeyType() {
        }

        public StreamingKeyType(Long longVal, Integer intVal, Short shortVal, Byte byteVal, String strVal) {
            this.longVal = longVal;
            this.intVal = intVal;
            this.shortVal = shortVal;
            this.byteVal = byteVal;
            this.strVal = strVal;
        }

        public Long getLongVal() {
            return longVal;
        }

        public void setLongVal(Long longVal) {
            this.longVal = longVal;
        }

        public Integer getIntVal() {
            return intVal;
        }

        public void setIntVal(Integer intVal) {
            this.intVal = intVal;
        }

        public Short getShortVal() {
            return shortVal;
        }

        public void setShortVal(Short shortVal) {
            this.shortVal = shortVal;
        }

        public Byte getByteVal() {
            return byteVal;
        }

        public void setByteVal(Byte byteVal) {
            this.byteVal = byteVal;
        }

        public String getStrVal() {
            return strVal;
        }

        public void setStrVal(String strVal) {
            this.strVal = strVal;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StreamingKeyType that = (StreamingKeyType) o;
            return Objects.equals(longVal, that.longVal) && Objects.equals(intVal, that.intVal) && Objects.equals(shortVal, that.shortVal) && Objects.equals(byteVal, that.byteVal) && Objects.equals(strVal, that.strVal);
        }

        @Override
        public int hashCode() {
            return Objects.hash(longVal, intVal, shortVal, byteVal, strVal);
        }
    }

    private static class OrderedEchoSink extends RichSinkFunction<StreamingKeyType> {

        private static final List<StreamingKeyType> orderedData = Lists.newArrayList();

        @Override
        public void invoke(StreamingKeyType value, SinkFunction.Context ctx) throws Exception {
            orderedData.add(value);
        }


        public List<StreamingKeyType> getOrderedData() {
            return orderedData;
        }
    }

    private static class EchoSink extends RichSinkFunction<UserType> {
        private static final Set<UserType> collectedData = Sets.newHashSet();

        @Override
        public void invoke(UserType value, SinkFunction.Context ctx) throws Exception {
            collectedData.add(value);
        }

        public Set<UserType> getCollectedData() {
            return collectedData;
        }
    }
}
