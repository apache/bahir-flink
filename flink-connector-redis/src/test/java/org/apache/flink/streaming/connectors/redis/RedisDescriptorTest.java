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

package org.apache.flink.streaming.connectors.redis;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.descriptor.Redis;
import org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;

public class RedisDescriptorTest extends  RedisITCaseBase{

    private static final String REDIS_KEY = "TEST_KEY";

    StreamExecutionEnvironment env;

    @Before
    public void setUp(){
        env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    @Test
    public void testRedisDescriptor() throws Exception {
        DataStreamSource<Row> source = (DataStreamSource<Row>) env.addSource(new TestSourceFunctionString())
                .returns(new RowTypeInfo(TypeInformation.of(String.class), TypeInformation.of(Long.class)));

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
        tableEnvironment.registerDataStream("t1", source, "k, v");

        Redis redis = new Redis()
                .mode(RedisValidator.REDIS_CLUSTER)
                .command(RedisCommand.INCRBY_EX.name())
                .ttl(100000)
                .property(RedisValidator.REDIS_NODES, REDIS_HOST+ ":" + REDIS_PORT);

        tableEnvironment
                .connect(redis).withSchema(new Schema()
                .field("k", TypeInformation.of(String.class))
                .field("v", TypeInformation.of(Long.class)))
                .createTemporaryTable("redis");

        tableEnvironment.executeSql("insert into redis select k, v from t1");
    }

    @Test
    public void testRedisTableFactory() throws Exception {
        DataStreamSource<Row> source = (DataStreamSource<Row>) env.addSource(new TestSourceFunctionString())
                .returns(new RowTypeInfo(TypeInformation.of(String.class), TypeInformation.of(Long.class)));

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        Table table = tableEnv.fromDataStream(source);
        tableEnv.createTemporaryView("t1", table);

        tableEnv.executeSql("CREATE TABLE redis (key  STRING, number BIGINT) WITH ('connector.type'='redis'," +
                "'redis-mode'='cluster', 'key.ttl' = '70000','command'='INCRBY_EX','cluster-nodes'='" + REDIS_HOST + ":" + REDIS_PORT + "')");

        tableEnv.executeSql("insert into redis select * from t1");

    }

    private static class TestSourceFunctionString implements SourceFunction<Row> {
        private static final long serialVersionUID = 1L;

        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Row> ctx) throws Exception {
            while (running) {
                Row row = new Row(2);
                row.setField(0, REDIS_KEY);
                row.setField(1, 2L);
                ctx.collect(row);
                Thread.sleep(2000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

}
