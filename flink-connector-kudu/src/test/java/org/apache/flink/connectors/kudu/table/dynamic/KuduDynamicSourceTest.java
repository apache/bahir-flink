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
package org.apache.flink.connectors.kudu.table.dynamic;

import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.KuduTestBase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Unit Tests for {@link KuduDynamicTableSource}.
 */
public class KuduDynamicSourceTest extends KuduTestBase {
    public static final String INPUT_TABLE = "books";
    public static StreamExecutionEnvironment env;
    public static TableEnvironment tEnv;

    @BeforeEach
    public void init() {
        KuduTableInfo tableInfo = booksTableInfo(INPUT_TABLE, true);
        setUpDatabase(tableInfo);
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
    }

    @AfterEach
    public void clean() {
        KuduTableInfo tableInfo = booksTableInfo(INPUT_TABLE, true);
        cleanDatabase(tableInfo);
    }

    @Test
    public void testKuduSource() throws Exception {
        // "id", "title", "author", "price", "quantity"
        tEnv.executeSql(
                "CREATE TABLE "
                        + INPUT_TABLE
                        + "("
                        + "id int,"
                        + "title string,"
                        + "author string,"
                        + "price double,"
                        + "quantity int"
                        + ") WITH ("
                        + "  'connector'='kudu',"
                        + "  'kudu.masters'='"
                        + getMasterAddress()
                        + "',"
                        + "  'kudu.table'='"
                        + INPUT_TABLE
                        + "',"
                        + "'kudu.scan.row-size'='10'," +
                        "'kudu.primary-key-columns'='id'"
                        + ")");

        Iterator<Row> collected = tEnv.executeSql("SELECT * FROM " + INPUT_TABLE).collect();
        assertNotNull(collected);
    }

    @Test
    public void testProject() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE "
                        + INPUT_TABLE
                        + "("
                        + "id int,"
                        + "title string,"
                        + "author string,"
                        + "price double,"
                        + "quantity int"
                        + ") WITH ("
                        + "  'connector'='kudu',"
                        + "  'kudu.masters'='"
                        + getMasterAddress()
                        + "',"
                        + "  'kudu.table'='"
                        + INPUT_TABLE
                        + "',"
                        + "'kudu.scan.row-size'='10'," +
                        "'kudu.primary-key-columns'='id'"
                        + ")");

        Iterator<Row> collected =
                tEnv.executeSql("SELECT id,title,author FROM " + INPUT_TABLE)
                        .collect();
        assertNotNull(collected);
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        List<String> expected =
                Stream.of(
                                "+I[1001, Java for dummies, Tan Ah Teck]",
                                "+I[1002, More Java for dummies, Tan Ah Teck]",
                                "+I[1003, More Java for more dummies, Mohammad Ali]",
                                "+I[1004, A Cup of Java, Kumar]",
                                "+I[1005, A Teaspoon of Java, Kevin Jones]")
                        .sorted()
                        .collect(Collectors.toList());
        assertEquals(expected, result);
    }

    @Test
    public void testLimit() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE "
                        + INPUT_TABLE
                        + "("
                        + "id int,"
                        + "title string,"
                        + "author string,"
                        + "price double,"
                        + "quantity int"
                        + ") WITH ("
                        + "  'connector'='kudu',"
                        + "  'kudu.masters'='"
                        + getMasterAddress()
                        + "',"
                        + "  'kudu.table'='"
                        + INPUT_TABLE
                        + "',"
                        + "'kudu.scan.row-size'='10'," +
                        "'kudu.primary-key-columns'='id'"
                        + ")");

        Iterator<Row> collected =
                tEnv.executeSql("SELECT * FROM " + INPUT_TABLE + " LIMIT 1").collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        assertEquals(1, result.size());
    }
}
