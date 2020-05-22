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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

/**
 * Creates a TableSource to connect an ActiveMQ Queue or Topic
 *
 * <p> The AMQTableSource is used as shown in the example below
 *
 * <pre>
 * {@code
 * AMQSourceConfig<Row> conf = ...
 * String[] fields = new String[]{"f1","f2"};
 * DataType[] types = new DataType[]{DataTypes.STRING(), DataTypes.BIGINT()};
 * TableSchema schema = TableSchema.builder().fields(fields,types).build();
 * AMQTableSource ats = new AMQTableSource(conf, schema);
 *
 * tableEnv.registerTableSource("amqTable", ats);
 * Table res = tableEnv.sqlQuery("select f1 from amqTable");
 * }
 * </pre>
 */
public class AMQTableSource implements StreamTableSource<Row> {
    private final TableSchema schema;
    private final AMQSourceConfig<Row> config;

    /**
     * The ActiveMQ configuration and the schema of TableSource
     *
     * @param config        ActiveMQ configuration
     * @param schema        the table schema
     */
    public AMQTableSource(AMQSourceConfig<Row> config, TableSchema schema){
        this.config = config;
        this.schema = schema;
    }


    @Override
    public TypeInformation<Row> getReturnType() {
        return schema.toRowType();
    }

    @Override
    public TableSchema getTableSchema() {
        return schema;
    }


    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {
        return env.addSource(new AMQSource<>(config));
    }

}
