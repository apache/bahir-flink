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
package org.apache.flink.streaming.connectors.kudu.connect;

import org.apache.commons.collections.map.HashedMap;
import org.apache.kudu.Type;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.KuduTable;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class KuduContextTest {

    private final String hostsCluster = "172.25.0.6";

    private KuduTableInfo obtainKuduTableInfo(boolean createIfNotExist) {
        return KuduTableInfo.Builder
                .create("pruebas")
                .createIfNotExist(createIfNotExist)
                .replicas(1)
                .addColumn(KuduColumnInfo.Builder.create("key", Type.INT32).key(true).hashKey(true).build())
                .addColumn(KuduColumnInfo.Builder.create("value", Type.STRING).build())
                .build();
    }

    @Test
    public void testTableCreationAndDeletion() throws Exception {
        AsyncKuduClient client = KuduConnection.getAsyncClient(hostsCluster);
        KuduTable table = KuduTableContext.getKuduTable(client, obtainKuduTableInfo(true));



        Assert.assertTrue("table dont exists", table != null);
        Assert.assertTrue("table not eliminated", KuduTableContext.deleteKuduTable(client, obtainKuduTableInfo(true)));

    }


    @Test(expected = UnsupportedOperationException.class)
    public void testTableCreationError() throws Exception {
        AsyncKuduClient client = KuduConnection.getAsyncClient(hostsCluster);
        KuduTable table = KuduTableContext.getKuduTable(client, obtainKuduTableInfo(false));
    }

    /*
    private KuduRow createRow(Integer key) {
        Map<String, Object> map = new HashedMap<>();
        map.put("key", key);
        map.put("value", "value"+key);
        return new KuduRow(map);
    }
*/
}
