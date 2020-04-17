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
import org.apache.flink.table.api.TableEnvironment;

import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.RowResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KuduTableFactoryTest extends KuduTestBase {
    private TableEnvironment tableEnv;
    private String kuduMasters;

    @BeforeEach
    public void init() {
        tableEnv = KuduTableTestUtils.createTableEnvWithBlinkPlannerBatchMode();
        kuduMasters = harness.getMasterAddressesAsString();
    }

    @Test
    public void testMissingTable() throws Exception {
        tableEnv.sqlUpdate("CREATE TABLE TestTable11 (`first` STRING, `second` INT) " +
                "WITH ('connector.type'='kudu', 'kudu.masters'='" + kuduMasters + "')");
        tableEnv.sqlUpdate("INSERT INTO TestTable11 values ('f', 1)");
        assertThrows(NullPointerException.class,
                () -> tableEnv.execute("test"));
    }

    @Test
    public void testMissingMasters() throws Exception {
        tableEnv.sqlUpdate("CREATE TABLE TestTable11 (`first` STRING, `second` INT) " +
                "WITH ('connector.type'='kudu', 'kudu.table'='TestTable11')");
        tableEnv.sqlUpdate("INSERT INTO TestTable11 values ('f', 1)");
        assertThrows(NullPointerException.class,
                () -> tableEnv.execute("test"));
    }

    @Test
    public void testNonExistingTable() throws Exception {
        tableEnv.sqlUpdate("CREATE TABLE TestTable11 (`first` STRING, `second` INT) " +
                "WITH ('connector.type'='kudu', 'kudu.table'='TestTable11', 'kudu.masters'='" + kuduMasters + "')");
        tableEnv.sqlUpdate("INSERT INTO TestTable11 values ('f', 1)");
        assertThrows(java.util.concurrent.ExecutionException.class,
                () -> tableEnv.execute("test"));
    }

    @Test
    public void testCreateTable() throws Exception {
        tableEnv.sqlUpdate("CREATE TABLE TestTable11 (`first` STRING, `second` STRING) " +
                "WITH ('connector.type'='kudu', 'kudu.table'='TestTable11', 'kudu.masters'='" + kuduMasters + "', " +
                "'kudu.hash-columns'='first', 'kudu.primary-key-columns'='first')");
        tableEnv.sqlUpdate("INSERT INTO TestTable11 values ('f', 's')");
        tableEnv.execute("test");

        validateSingleKey("TestTable11");
    }

    @Test
    public void testExistingTable() throws Exception {
        // Creating a table
        tableEnv.sqlUpdate("CREATE TABLE TestTable12 (`first` STRING, `second` STRING) " +
                "WITH ('connector.type'='kudu', 'kudu.table'='TestTable12', 'kudu.masters'='" + kuduMasters + "', " +
                "'kudu.hash-columns'='first', 'kudu.primary-key-columns'='first')");

        tableEnv.sqlUpdate("INSERT INTO TestTable12 values ('f', 's')");
        tableEnv.execute("test");

        // Then another one in SQL that refers to the previously created one
        tableEnv.sqlUpdate("CREATE TABLE TestTable12b (`first` STRING, `second` STRING) " +
                "WITH ('connector.type'='kudu', 'kudu.table'='TestTable12', 'kudu.masters'='" + kuduMasters + "')");
        tableEnv.sqlUpdate("INSERT INTO TestTable12b values ('f2','s2')");
        tableEnv.execute("test2");

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
