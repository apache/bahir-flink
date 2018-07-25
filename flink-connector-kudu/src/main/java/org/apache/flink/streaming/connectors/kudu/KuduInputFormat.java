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

import org.apache.flink.api.common.io.LocatableInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.streaming.connectors.kudu.connect.*;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KuduInputFormat extends RichInputFormat<Row, KuduInputSplit>
        implements ResultTypeQueryable<Row> {

    private transient KuduScanner scanner;

    private String kuduMasters;
    private KuduTableInfo tableInfo;
    private List<KuduFilter> tableFilters;
    private List<String> tableProjections;

    private transient AsyncKuduClient kuduClient;
    private transient KuduTable kuduTable;

    private transient RowResultIterator resultIterator;

    private boolean endReached = false;


    private static final Logger LOG = LoggerFactory.getLogger(KuduInputFormat.class);

    public KuduInputFormat(String kuduMasters, KuduTableInfo tableInfo) {
        Preconditions.checkNotNull(kuduMasters,"kuduMasters could not be null");
        this.kuduMasters = kuduMasters;

        Preconditions.checkNotNull(tableInfo,"tableInfo could not be null");
        this.tableInfo = tableInfo;
    }

    @Override
    public void configure(Configuration parameters) {
        /*
        this.kuduClient = new KuduClient.KuduClientBuilder(conf.masterAddress)
                .build();
        try {
            this.kuduTable = this.kuduClient.openTable(conf.tableName);
        } catch (KuduException e) {
            e.printStackTrace();
        }
        this.predicates = toKuduPredicates(conf.predicates, this.kuduTable.getSchema());
        */
    }

    @Override
    public void open(KuduInputSplit split) throws IOException {
        kuduClient = KuduConnection.getAsyncClient(kuduMasters);
        kuduTable = KuduTableContext.getKuduTable(kuduClient, tableInfo);

        /**/
        endReached = false;

        scanner = KuduScanToken.deserializeIntoScanner(split.getScanToken(), kuduClient.syncClient());

        resultIterator = scanner.nextRows();
    }

    @Override
    public void close() {
        KuduConnection.closeAsyncClient(kuduMasters);

        /**/
        if (scanner != null) {
            try {
                scanner.close();
            } catch (KuduException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return cachedStatistics;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(KuduInputSplit[] inputSplits) {
        return new LocatableInputSplitAssigner(inputSplits);
    }

    @Override
    public KuduInputSplit[] createInputSplits(int minNumSplits) throws IOException {
        Preconditions.checkNotNull(kuduClient,"kuduClient could not be null");
        Preconditions.checkNotNull(kuduTable,"kuduTable could not be null");

        /**/

        KuduScanToken.KuduScanTokenBuilder tokenBuilder = getKuduScanTokenBuilder();

        List<KuduScanToken> tokens = tokenBuilder.build();

        KuduInputSplit[] splits = new KuduInputSplit[tokens.size()];

        for (int i = 0; i < tokens.size(); i++) {
            KuduScanToken token = tokens.get(i);

            List<String> locations = new ArrayList<>(token.getTablet().getReplicas().size());

            for (LocatedTablet.Replica replica : token.getTablet().getReplicas()) {
                locations.add(getLocation(replica.getRpcHost(), replica.getRpcPort()));
            }

            KuduInputSplit split = new KuduInputSplit(
                    token.serialize(),
                    i,
                    locations.toArray(new String[locations.size()])
            );
            splits[i] = split;
        }

        if (splits.length < minNumSplits) {
            LOG.warn(" The minimum desired number of splits with your configured parallelism level " +
                            "is {}. Current kudu splits = {}. {} instances will remain idle.",
                    minNumSplits,
                    splits.length,
                    (minNumSplits - splits.length)
            );
        }

        return splits;
    }




    @Override
    public boolean reachedEnd() throws IOException {
        return endReached;
    }

    @Override
    public Row nextRecord(Row reuse) throws IOException {
        // check that current iterator has next rows
        if (this.resultIterator.hasNext()) {
            RowResult row = this.resultIterator.next();
            return rowResultToTuple(row);
        }
        // if not, check that current scanner has more iterators
        else if (scanner.hasMoreRows()) {
            this.resultIterator = scanner.nextRows();
            return nextRecord(reuse);
        }
        else {
            endReached = true;
        }
        return null;
    }

    public static Row rowResultToTuple(RowResult row) {
        Schema columnProjection = row.getColumnProjection();
        int columns = columnProjection.getColumnCount();

        Row tuple = new Row(columns);

        for (int i = 0; i < columns; i++) {
            Type type = row.getColumnType(i);
            switch (type) {
                case BINARY:
                    tuple.setField(i, row.getBinary(i));
                    break;
                case STRING:
                    tuple.setField(i, row.getString(i));
                    break;
                case BOOL:
                    tuple.setField(i, row.getBoolean(i));
                    break;
                case DOUBLE:
                    tuple.setField(i, row.getDouble(i));
                    break;
                case FLOAT:
                    tuple.setField(i, row.getFloat(i));
                    break;
                case INT8:
                    tuple.setField(i, row.getByte(i));
                    break;
                case INT16:
                    tuple.setField(i, row.getShort(i));
                    break;
                case INT32:
                    tuple.setField(i, row.getInt(i));
                    break;
                case INT64:
                case UNIXTIME_MICROS:
                    tuple.setField(i, row.getLong(i));
                    break;
                default:
                    throw new IllegalArgumentException("Illegal var type: " + type);
            }
        }
        return tuple;
    }




    /**
     * Returns a endpoint url in the following format: <host>:<ip>
     *
     * @param host Hostname
     * @param port Port
     * @return Formatted URL
     */
    private String getLocation(String host, Integer port) {
        StringBuilder builder = new StringBuilder();
        builder.append(host).append(":").append(port);
        return builder.toString();
    }

    private KuduScanToken.KuduScanTokenBuilder getKuduScanTokenBuilder() {
        KuduScanToken.KuduScanTokenBuilder tokenBuilder = kuduClient
                .syncClient()
                .newScanTokenBuilder(kuduTable);

        if (tableProjections != null && tableProjections.size() > 0) {
            tokenBuilder.setProjectedColumnNames(tableProjections);
        }

        for (KuduFilter predicate : tableFilters) {
            tokenBuilder.addPredicate(KuduUtils.predicate(predicate, kuduTable.getSchema()));
        }

        /*
        if (conf.limit > 0) {
            tokenBuilder.limit(conf.limit);
            // FIXME: https://issues.apache.org/jira/browse/KUDU-16
            // Server side limit() operator for java-based scanners are not implemented yet

        }
*/
        return tokenBuilder;
    }


    @Override
    public TypeInformation<Row> getProducedType() {
        return null;
    }
}
