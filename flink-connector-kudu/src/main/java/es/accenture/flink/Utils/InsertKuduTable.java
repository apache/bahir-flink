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

package es.accenture.flink.Utils;

import org.apache.kudu.client.*;


public class InsertKuduTable {

    public static void main(String[] args) {

        String tableName = ""; // TODO insert table name
        String host = "localhost";

        long startTime = System.currentTimeMillis();
        KuduClient client = new KuduClient.KuduClientBuilder(host).build();
        insertToKudu(client, tableName);

        long endTime = System.currentTimeMillis();
        System.out.println("Program executed in " + (endTime - startTime)/1000 + " seconds");  //divide by 1000000 to get milliseconds.
    }


    private static void insertToKudu (KuduClient client, String tableName){
        try {
            KuduTable table = client.openTable(tableName);
            KuduSession session = client.newSession();
            session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
            for (int i = 0; i < 10000; i++) {
                Insert insert = table.newInsert();
                PartialRow row = insert.getRow();
                row.addInt(0, i);
                row.addString(1, "This is the row number: "+ i);
                session.apply(insert);
            }
            session.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
