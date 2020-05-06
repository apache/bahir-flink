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
package org.apache.flink.connectors.kudu.batch;

import org.apache.flink.connectors.kudu.connector.KuduTestBase;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.reader.KuduInputSplit;
import org.apache.flink.connectors.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class KuduInputFormatTest extends KuduTestBase {

    @Test
    void testInvalidKuduMaster() {
        KuduTableInfo tableInfo = booksTableInfo("books", false);
        Assertions.assertThrows(NullPointerException.class, () -> new KuduRowInputFormat(null, tableInfo));
    }

    @Test
    void testInvalidTableInfo() {
        String masterAddresses = harness.getMasterAddressesAsString();
        KuduReaderConfig readerConfig = KuduReaderConfig.Builder.setMasters(masterAddresses).build();
        Assertions.assertThrows(NullPointerException.class, () -> new KuduRowInputFormat(readerConfig, null));
    }

    @Test
    void testInputFormat() throws Exception {
        KuduTableInfo tableInfo = booksTableInfo("books", true);
        setUpDatabase(tableInfo);

        List<Row> rows = readRows(tableInfo);
        Assertions.assertEquals(5, rows.size());

        cleanDatabase(tableInfo);
    }

    @Test
    void testInputFormatWithProjection() throws Exception {
        KuduTableInfo tableInfo = booksTableInfo("books", true);
        setUpDatabase(tableInfo);

        List<Row> rows = readRows(tableInfo, "title", "id");
        Assertions.assertEquals(5, rows.size());

        for (Row row : rows) {
            Assertions.assertEquals(2, row.getArity());
        }

        cleanDatabase(tableInfo);
    }

    private List<Row> readRows(KuduTableInfo tableInfo, String... fieldProjection) throws Exception {
        String masterAddresses = harness.getMasterAddressesAsString();
        KuduReaderConfig readerConfig = KuduReaderConfig.Builder.setMasters(masterAddresses).build();
        KuduRowInputFormat inputFormat = new KuduRowInputFormat(readerConfig, tableInfo, new ArrayList<>(),
                fieldProjection == null ? null : Arrays.asList(fieldProjection));

        KuduInputSplit[] splits = inputFormat.createInputSplits(1);
        List<Row> rows = new ArrayList<>();
        for (KuduInputSplit split : splits) {
            inputFormat.open(split);
            while (!inputFormat.reachedEnd()) {
                Row row = inputFormat.nextRecord(new Row(5));
                if (row != null) {
                    rows.add(row);
                }
            }
        }
        inputFormat.close();

        return rows;
    }
}
