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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kudu.connector.KuduDatabase;
import org.apache.flink.streaming.connectors.kudu.connector.KuduRow;
import org.apache.flink.streaming.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.streaming.connectors.kudu.serde.DefaultSerDe;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

@DockerTest
public class KuduSinkTest extends KuduDatabase {

    @Test
    public void testInvalidKuduMaster() throws IOException {
        KuduTableInfo tableInfo = booksTableInfo(UUID.randomUUID().toString(),false);
        Assertions.assertThrows(NullPointerException.class, () -> new KuduOutputFormat<>(null, tableInfo, new DefaultSerDe()));
    }

    @Test
    public void testInvalidTableInfo() throws IOException {
        Assertions.assertThrows(NullPointerException.class, () -> new KuduOutputFormat<>(hostsCluster, null, new DefaultSerDe()));
    }

    @Test
    public void testNotTableExist() throws IOException {
        KuduTableInfo tableInfo = booksTableInfo(UUID.randomUUID().toString(),false);
        KuduSink sink = new KuduSink<>(hostsCluster, tableInfo, new DefaultSerDe());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> sink.open(new Configuration()));
    }

    @Test
    public void testOutputWithStrongConsistency() throws Exception {

        KuduTableInfo tableInfo = booksTableInfo(UUID.randomUUID().toString(),true);
        KuduSink sink = new KuduSink<>(hostsCluster, tableInfo, new DefaultSerDe())
                .withStrongConsistency();
        sink.open(new Configuration());

        for (KuduRow kuduRow : booksDataRow()) {
            sink.invoke(kuduRow);
        }
        sink.close();

        List<KuduRow> rows = KuduInputFormatTest.readRows(tableInfo);
        Assertions.assertEquals(5, rows.size());

    }

    @Test
    public void testOutputWithEventualConsistency() throws Exception {
        KuduTableInfo tableInfo = booksTableInfo(UUID.randomUUID().toString(),true);
        KuduSink sink = new KuduSink<>(hostsCluster, tableInfo, new DefaultSerDe())
                .withEventualConsistency();
        sink.open(new Configuration());

        for (KuduRow kuduRow : booksDataRow()) {
            sink.invoke(kuduRow);
        }

        // sleep to allow eventual consistency to finish
        Thread.sleep(1000);

        sink.close();

        List<KuduRow> rows = KuduInputFormatTest.readRows(tableInfo);
        Assertions.assertEquals(5, rows.size());
    }

}
