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

package es.accenture.flink.Sink;

import es.accenture.flink.Utils.Exceptions.KuduClientException;
import es.accenture.flink.Utils.RowSerializable;
import es.accenture.flink.Utils.Utils;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kudu.client.KuduTable;
import org.apache.log4j.Logger;

import java.io.IOException;

public class KuduSink extends RichSinkFunction<RowSerializable>{

    private String host, tableName;
    private String [] fieldsNames;
    private transient Utils utils;

    //Kudu variables
    private transient KuduTable table;

    // LOG4J

    private final static Logger logger = Logger.getLogger(KuduSink.class);

    /**
     * Builder to use when you want to create a new table
     *
     * @param host          Kudu host
     * @param tableName     Kudu table name
     * @param fieldsNames   List of column names in the table to be created
     * @throws KuduClientException In case of exception caused by Kudu Client
     */
    public KuduSink (String host, String tableName, String [] fieldsNames) throws KuduClientException {

        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("ERROR: Param \"host\" not valid (null or empty)");

        } else if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("ERROR: Param \"tableName\" not valid (null or empty)");

        }
        this.host = host;
        this.tableName = tableName;
        this.fieldsNames = fieldsNames;
    }

    /**
     * Builder to be used when using an existing table
     *
     * @param host          Kudu host
     * @param tableName     Kudu table name
     * @throws KuduClientException In case of exception caused by Kudu Client
     */
    public KuduSink (String host, String tableName) throws KuduClientException {

        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("ERROR: Param \"host\" not valid (null or empty)");

        } else if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("ERROR: Param \"tableName\" not valid (null or empty)");
        }

        this.host = host;
        this.tableName = tableName;
    }

    /**
     * It's responsible to insert a row into the indicated table by the builder (Streaming)
     *
     * @param row   Data of a row to insert
     */
    @Override
    public void invoke(RowSerializable row) throws IOException {

        // Establish connection with Kudu
        if (this.utils == null)
            this.utils = new Utils(host);

        if (this.table == null)
            this.table = this.utils.useTable(tableName, fieldsNames, row);


        // Make the insert into the table
        utils.insert(table, row, fieldsNames);

        logger.info("Inserted the Row: | " + utils.printRow(row) + "at the table \"" + this.tableName + "\"");
    }
}
