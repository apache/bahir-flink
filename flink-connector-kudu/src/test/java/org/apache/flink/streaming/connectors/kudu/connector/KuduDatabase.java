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
package org.apache.flink.streaming.connectors.kudu.connector;

import org.apache.kudu.Type;
import org.junit.jupiter.api.Assertions;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class KuduDatabase {

    protected static final String hostsCluster = "172.25.0.6";

    protected static final Object[][] booksTableData = {
            {1001, "Java for dummies", "Tan Ah Teck", 11.11, 11},
            {1002, "More Java for dummies", "Tan Ah Teck", 22.22, 22},
            {1003, "More Java for more dummies", "Mohammad Ali", 33.33, 33},
            {1004, "A Cup of Java", "Kumar", 44.44, 44},
            {1005, "A Teaspoon of Java", "Kevin Jones", 55.55, 55}};


    protected static KuduTableInfo booksTableInfo(String tableName, boolean createIfNotExist) {
        return KuduTableInfo.Builder
                .create(tableName)
                .createIfNotExist(createIfNotExist)
                .replicas(1)
                .addColumn(KuduColumnInfo.Builder.create("id", Type.INT32).key(true).hashKey(true).build())
                .addColumn(KuduColumnInfo.Builder.create("title", Type.STRING).build())
                .addColumn(KuduColumnInfo.Builder.create("author", Type.STRING).build())
                .addColumn(KuduColumnInfo.Builder.create("price", Type.DOUBLE).build())
                .addColumn(KuduColumnInfo.Builder.create("quantity", Type.INT32).build())
                .build();
    }

    protected static List<KuduRow> booksDataRow() {
        return Arrays.stream(booksTableData)
                .map(row -> {
                        KuduRow values = new KuduRow(5);
                        values.setField(0, "id", row[0]);
                        values.setField(1, "title", row[1]);
                        values.setField(2, "author", row[2]);
                        values.setField(3, "price", row[3]);
                        values.setField(4, "quantity", row[4]);
                        return values;
                })
                .collect(Collectors.toList());
    }

    public void setUpDatabase(KuduTableInfo tableInfo) {
        try {
            KuduConnector tableContext = new KuduConnector(hostsCluster, tableInfo);
            booksDataRow().forEach(row -> {
                try {
                    tableContext.writeRow(row, KuduConnector.Consistency.STRONG, KuduConnector.WriteMode.UPSERT);
                }catch (Exception e) {
                    e.printStackTrace();
                }
            });
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    protected static void cleanDatabase(KuduTableInfo tableInfo) {
        try {
            KuduConnector tableContext = new KuduConnector(hostsCluster, tableInfo);
            tableContext.deleteTable();
            tableContext.close();
        } catch (Exception e) {
            Assertions.fail();
        }
    }
}
