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
package org.apache.flink.streaming.connectors.kudu;

import org.apache.flink.streaming.connectors.kudu.connector.KuduDatabase;
import org.apache.flink.streaming.connectors.kudu.connector.KuduRow;
import org.apache.flink.streaming.connectors.kudu.connector.KuduTableInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KuduInputFormatTest extends KuduDatabase {




    @Test
    public void testInvalidKuduMaster() throws IOException {
        KuduTableInfo tableInfo = booksTableInfo("books",false);
        Assertions.assertThrows(NullPointerException.class, () -> new KuduInputFormat(null, tableInfo));
    }

    @Test
    public void testInvalidTableInfo() throws IOException {
        Assertions.assertThrows(NullPointerException.class, () -> new KuduInputFormat(hostsCluster, null));
    }

    @Test
    public void testInputFormat() throws Exception {
        KuduTableInfo tableInfo = booksTableInfo("books",true);
        setUpDatabase(tableInfo);

        List<KuduRow> rows = readRows(tableInfo);
        Assertions.assertEquals(5, rows.size());

        cleanDatabase(tableInfo);
    }

    @Test
    public void testInputFormatWithProjection() throws Exception {
        KuduTableInfo tableInfo = booksTableInfo("books",true);
        setUpDatabase(tableInfo);

        List<KuduRow> rows = readRows(tableInfo,"title","id");
        Assertions.assertEquals(5, rows.size());

        for (KuduRow row: rows) {
            Assertions.assertEquals(2, row.getArity());
        }

        cleanDatabase(tableInfo);
    }


    public static List<KuduRow> readRows(KuduTableInfo tableInfo, String... fieldProjection) throws Exception {
        KuduInputFormat inputFormat = new KuduInputFormat(hostsCluster, tableInfo)
                .withTableProjections(fieldProjection);

        KuduInputFormat.KuduInputSplit[] splits = inputFormat.createInputSplits(1);
        List<KuduRow> rows = new ArrayList<>();
        for (KuduInputFormat.KuduInputSplit split : splits) {
            inputFormat.open(split);
            while(!inputFormat.reachedEnd()) {
                KuduRow row = inputFormat.nextRecord(new KuduRow(5));
                if(row != null) {
                    rows.add(row);
                }
            }
        }
        inputFormat.close();

        return rows;
    }
}
