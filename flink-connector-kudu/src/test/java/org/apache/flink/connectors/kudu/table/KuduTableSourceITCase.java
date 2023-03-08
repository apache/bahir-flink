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

import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.KuduTestBase;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration tests for {@link KuduTableSource}.
 */
public class KuduTableSourceITCase extends KuduTestBase {
    private TableEnvironment tableEnv;
    private KuduCatalog catalog;

    @BeforeEach
    public void init() {
        KuduTableInfo tableInfo = booksTableInfo("books", true);
        setUpDatabase(tableInfo);
        tableEnv = KuduTableTestUtils.createTableEnvWithBlinkPlannerBatchMode();
        catalog = new KuduCatalog(getMasterAddress());
        tableEnv.registerCatalog("kudu", catalog);
        tableEnv.useCatalog("kudu");
    }

    @Test
    void testFullBatchScan() throws Exception {
        CloseableIterator<Row> it = tableEnv.executeSql("select * from books order by id").collect();
        List<Row> results = new ArrayList<>();
        it.forEachRemaining(results::add);
        assertEquals(5, results.size());
        assertEquals("1001,Java for dummies,Tan Ah Teck,11.11,11", results.get(0).toString());
        tableEnv.executeSql("DROP TABLE books");
    }


    @Test
    void testScanWithProjectionAndFilter() throws Exception {
        // (price > 30 and price < 40)
        CloseableIterator<Row> it = tableEnv.executeSql("SELECT title FROM books WHERE id IN (1003, 1004) and " +
                "quantity < 40").collect();
        List<Row> results = new ArrayList<>();
        it.forEachRemaining(results::add);
        assertEquals(1, results.size());
        assertEquals("More Java for more dummies", results.get(0).toString());
        tableEnv.executeSql("DROP TABLE books");
    }
}
