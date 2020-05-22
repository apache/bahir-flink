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

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ActiveMQTableITCase {
    private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    private static final EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
    private static final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, bsSettings);

    private static final String TABLE_CREATE_SQL = "CREATE TABLE books (" +
            " id int, " +
            " title varchar, " +
            " author varchar, " +
            " price double, " +
            " qty int " +
            ") with (" +
            " 'connector.type' = 'activemq', " +
            " 'connector.broker-url' = 'vm://localhost?broker.persistent=false', " +
            " 'connector.destination-type' = 'QUEUE', " +
            " 'connector.destination-name' = 'source_queue' " +
            ")";

    private static final String INITIALIZE_TABLE_SQL = "INSERT INTO books VALUES\n" +
            "(1001, 'Java public for dummies', 'Tan Ah Teck', 11.11, 11),\n" +
            "(1002, 'More Java for dummies', 'Tan Ah Teck', 22.22, 22),\n" +
            "(1003, 'More Java for more dummies', 'Mohammad Ali', 33.33, 33),\n" +
            "(1004, 'A Cup of Java', 'Kumar', 44.44, 44),\n" +
            "(1005, 'A Teaspoon of Java', 'Kevin Jones', 55.55, 55),\n" +
            "(1006, 'A Teaspoon of Java 1.4', 'Kevin Jones', 66.66, 66),\n" +
            "(1007, 'A Teaspoon of Java 1.5', 'Kevin Jones', 77.77, 77),\n" +
            "(1008, 'A Teaspoon of Java 1.6', 'Kevin Jones', 88.88, 88),\n" +
            "(1009, 'A Teaspoon of Java 1.7', 'Kevin Jones', 99.99, 99),\n" +
            "(1010, 'A Teaspoon of Java 1.8', 'Kevin Jones', 33.33, 100)";

    private static final String QUERY_TABLE_SQL = "SELECT * FROM books";


    @Test
    public void testActiveMQSourceSink() throws Exception{

        // create activemq source table
        tEnv.sqlUpdate(TABLE_CREATE_SQL);

        // produce event to activemq
        tEnv.sqlUpdate(INITIALIZE_TABLE_SQL);

        // consume from activemq
        Table table = tEnv.sqlQuery(QUERY_TABLE_SQL);
        DataStream<Row> result = tEnv.toAppendStream(table, Row.class);
        result.addSink(new TestingSinkFunction(10)).setParallelism(1);

        try {
            env.execute("AMQTest");
        } catch (Throwable e) {
            // we have to use a specific exception to indicate the job is finished,
            // because the activemq source is infinite.
            if (!isCausedByJobFinished(e)) {
                // re-throw
                throw e;
            }
        }

        List<String> expected = Arrays.asList(
                "1001,Java public for dummies,Tan Ah Teck,11.11,11",
                "1002,More Java for dummies,Tan Ah Teck,22.22,22",
                "1003,More Java for more dummies,Mohammad Ali,33.33,33",
                "1004,A Cup of Java,Kumar,44.44,44",
                "1005,A Teaspoon of Java,Kevin Jones,55.55,55",
                "1006,A Teaspoon of Java 1.4,Kevin Jones,66.66,66",
                "1007,A Teaspoon of Java 1.5,Kevin Jones,77.77,77",
                "1008,A Teaspoon of Java 1.6,Kevin Jones,88.88,88",
                "1009,A Teaspoon of Java 1.7,Kevin Jones,99.99,99",
                "1010,A Teaspoon of Java 1.8,Kevin Jones,33.33,100"
        );

        Assert.assertEquals(expected, TestingSinkFunction.rows);
    }

    private static final class TestingSinkFunction implements SinkFunction<Row> {

        private static final long serialVersionUID = 455430015321124493L;
        private static List<String> rows = new ArrayList<>();

        private final int expectedSize;

        private TestingSinkFunction(int expectedSize) {
            this.expectedSize = expectedSize;
            rows.clear();
        }

        @Override
        public void invoke(Row value, Context context) throws Exception {
            rows.add(value.toString());
            if (rows.size() >= expectedSize) {
                Collections.sort(TestingSinkFunction.rows);
                // job finish
                throw new SuccessException();
            }
        }
    }

    private static boolean isCausedByJobFinished(Throwable e) {
        if (e instanceof SuccessException) {
            return true;
        } else if (e.getCause() != null) {
            return isCausedByJobFinished(e.getCause());
        } else {
            return false;
        }
    }
}
