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
package org.apache.flink.connectors.kudu.table;

import org.apache.flink.connectors.kudu.connector.KuduTestBase;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.shaded.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class KuduTableFactoryTest extends KuduTestBase {
    private StreamTableEnvironment tableEnv;
    private String kuduMasters;

    @BeforeEach
    public void init() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        tableEnv = KuduTableTestUtils.createTableEnvWithBlinkPlannerStreamingMode(env);
        kuduMasters = harness.getMasterAddressesAsString();
    }

    @Test
    public void testMissingMasters() throws Exception {
        tableEnv.executeSql("CREATE TABLE TestTable11 (`first` STRING, `second` INT) " +
                "WITH ('connector.type'='kudu', 'kudu.table'='TestTable11')");
        assertThrows(TableException.class,
                () -> tableEnv.executeSql("INSERT INTO TestTable11 values ('f', 1)"));
    }

    @Test
    public void testNonExistingTable() throws Exception {
        tableEnv.executeSql("CREATE TABLE TestTable11 (`first` STRING, `second` INT) " +
                "WITH ('connector.type'='kudu', 'kudu.table'='TestTable11', 'kudu.masters'='" + kuduMasters + "')");
        JobClient jobClient = tableEnv.executeSql("INSERT INTO TestTable11 values ('f', 1)").getJobClient().get();
        try {
            jobClient.getJobExecutionResult().get();
            fail();
        } catch (ExecutionException ee) {
            assertTrue(ee.getCause() instanceof JobExecutionException);
        }
    }

    @Test
    public void testCreateTable() throws Exception {
        tableEnv.executeSql("CREATE TABLE TestTable11 (`first` STRING, `second` STRING) " +
                "WITH ('connector.type'='kudu', 'kudu.table'='TestTable11', 'kudu.masters'='" + kuduMasters + "', " +
                "'kudu.hash-columns'='first', 'kudu.primary-key-columns'='first')");

        tableEnv.executeSql("INSERT INTO TestTable11 values ('f', 's')")
                .getJobClient()
                .get()
                .getJobExecutionResult()
                .get(1, TimeUnit.MINUTES);

        validateSingleKey("TestTable11");
    }

    @Test
    public void testTimestamp() throws Exception {
        // Timestamp should be bridged to sql.Timestamp
        // Test it when creating the table...
        tableEnv.executeSql("CREATE TABLE TestTableTs (`first` STRING, `second` TIMESTAMP(3)) " +
                "WITH ('connector.type'='kudu', 'kudu.masters'='" + kuduMasters + "', " +
                "'kudu.hash-columns'='first', 'kudu.primary-key-columns'='first')");
        tableEnv.executeSql("INSERT INTO TestTableTs values ('f', TIMESTAMP '2020-01-01 12:12:12.123456')")
                .getJobClient()
                .get()
                .getJobExecutionResult()
                .get(1, TimeUnit.MINUTES);

        tableEnv.executeSql("INSERT INTO TestTableTs values ('s', TIMESTAMP '2020-02-02 23:23:23')")
                .getJobClient()
                .get()
                .getJobExecutionResult()
                .get(1, TimeUnit.MINUTES);

        KuduTable kuduTable = harness.getClient().openTable("TestTableTs");
        assertEquals(Type.UNIXTIME_MICROS, kuduTable.getSchema().getColumn("second").getType());

        KuduScanner scanner = harness.getClient().newScannerBuilder(kuduTable).build();
        HashSet<Timestamp> results = new HashSet<>();
        scanner.forEach(sc -> results.add(sc.getTimestamp("second")));

        assertEquals(2, results.size());
        List<Timestamp> expected = Lists.newArrayList(
                Timestamp.valueOf("2020-01-01 12:12:12.123"),
                Timestamp.valueOf("2020-02-02 23:23:23"));
        assertEquals(new HashSet<>(expected), results);
    }

    @Test
    public void testExistingTable() throws Exception {
        // Creating a table
        tableEnv.executeSql("CREATE TABLE TestTable12 (`first` STRING, `second` STRING) " +
                "WITH ('connector.type'='kudu', 'kudu.table'='TestTable12', 'kudu.masters'='" + kuduMasters + "', " +
                "'kudu.hash-columns'='first', 'kudu.primary-key-columns'='first')");

        tableEnv.executeSql("INSERT INTO TestTable12 values ('f', 's')")
                .getJobClient()
                .get()
                .getJobExecutionResult()
                .get(1, TimeUnit.MINUTES);

        // Then another one in SQL that refers to the previously created one
        tableEnv.executeSql("CREATE TABLE TestTable12b (`first` STRING, `second` STRING) " +
                "WITH ('connector.type'='kudu', 'kudu.table'='TestTable12', 'kudu.masters'='" + kuduMasters + "')");
        tableEnv.executeSql("INSERT INTO TestTable12b values ('f2','s2')")
                .getJobClient()
                .get()
                .getJobExecutionResult()
                .get(1, TimeUnit.MINUTES);

        // Validate that both insertions were into the same table
        KuduTable kuduTable = harness.getClient().openTable("TestTable12");
        KuduScanner scanner = harness.getClient().newScannerBuilder(kuduTable).build();
        List<RowResult> rows = new ArrayList<>();
        scanner.forEach(rows::add);

        assertEquals(2, rows.size());
        assertEquals("f", rows.get(0).getString("first"));
        assertEquals("s", rows.get(0).getString("second"));
        assertEquals("f2", rows.get(1).getString("first"));
        assertEquals("s2", rows.get(1).getString("second"));
    }
}
