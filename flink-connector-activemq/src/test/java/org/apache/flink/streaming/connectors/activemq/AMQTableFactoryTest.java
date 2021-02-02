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

package org.apache.flink.streaming.connectors.activemq;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class AMQTableFactoryTest {

    private static final TableSchema SCHEMA = TableSchema.builder()
            .field("id", DataTypes.INT())
            .field("title", DataTypes.STRING())
            .field("author", DataTypes.STRING())
            .field("price", DataTypes.DOUBLE())
            .field("qty", DataTypes.INT()).build();

    private static final String SCHEMA_STRING = "root\n" +
            " |-- id: INT\n" +
            " |-- title: STRING\n" +
            " |-- author: STRING\n" +
            " |-- price: DOUBLE\n" +
            " |-- qty: INT\n";

    private DescriptorProperties createDescriptor(TableSchema schema){
        HashMap<String, String> tableProps = new HashMap<>();
        tableProps.put("connector.type","activemq");
        tableProps.put("connector.broker-url","vm://localhost?broker.persistent=false");
        tableProps.put("connector.destination-type", "QUEUE");
        tableProps.put("connector.destination-name", "queue_name");
        DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putTableSchema("schema", schema);
        descriptorProperties.putProperties(tableProps);
        return descriptorProperties;
    }

    @Test
    public void testTableSourceFactory(){
        DescriptorProperties descriptor = createDescriptor(SCHEMA);
        TableSource<Row> source = TableFactoryService.find(AMQTableFactory.class, descriptor.asMap())
                .createTableSource(descriptor.asMap());
        TableSchema tableSchema = source.getTableSchema();

        Assert.assertTrue(source instanceof AMQTableSource);
        Assert.assertEquals(5, tableSchema.getFieldCount());
        Assert.assertEquals(SCHEMA_STRING, tableSchema.toString());
    }

    @Test
    public void testTableSinkFactory() {
        DescriptorProperties descriptor = createDescriptor(SCHEMA);
        TableSink<Row> sink = TableFactoryService.find(AMQTableFactory.class, descriptor.asMap())
                .createTableSink(descriptor.asMap());
        TableSchema tableSchema = sink.getTableSchema();

        Assert.assertTrue(sink instanceof AMQTableSink);
        Assert.assertEquals(5, tableSchema.getFieldCount());
        Assert.assertEquals(SCHEMA_STRING, tableSchema.toString());
    }
}
