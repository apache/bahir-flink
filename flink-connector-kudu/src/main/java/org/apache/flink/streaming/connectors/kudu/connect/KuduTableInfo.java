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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.CreateTableOptions;

public class KuduTableInfo implements Serializable {

    private static final Integer DEFAULT_REPLICAS = 3;
    private static final boolean DEFAULT_CREATE_IF_NOT_EXIST = false;
    private static final Mode DEFAULT_MODE = Mode.UPSERT;

    public enum Mode {INSERT,UPDATE,UPSERT}


    private Integer replicas;
    private String name;
    private Mode mode;
    private boolean createIfNotExist;
    private List<KuduColumnInfo> columns;

    private KuduTableInfo(String name){
        this.name = name;
        this.replicas = DEFAULT_REPLICAS;
        this.createIfNotExist = DEFAULT_CREATE_IF_NOT_EXIST;
        this.columns = new ArrayList<>();
        this.mode = DEFAULT_MODE;
    }

    public String getName() {
        return name;
    }

    public Mode getMode() {
        return mode;
    }

    public boolean isUpsertMode() {
        return Mode.UPSERT.equals(getMode());
    }

    public boolean isInsertMode() {
        return Mode.INSERT.equals(getMode());
    }

    public boolean isUpdateMode() {
        return Mode.UPDATE.equals(getMode());
    }

    public Schema getSchema() {
        if(hasNotColumns()) return null;
        List<ColumnSchema> schemaColumns = new ArrayList<>();
        for(KuduColumnInfo column : columns){
            schemaColumns.add(column.columnSchema());
        }
        return new Schema(schemaColumns);
    }

    public boolean createIfNotExist() {
        return createIfNotExist;
    }

    public CreateTableOptions getCreateTableOptions() {
        CreateTableOptions options = new CreateTableOptions();
        if(replicas!=null){
            options.setNumReplicas(replicas);
        }
        if(hasColummns()) {
            List<String> rangeKeys = new ArrayList<>();
            List<String> hashKeys = new ArrayList<>();
            for(KuduColumnInfo column : columns){
                if(column.rangeKey){
                    rangeKeys.add(column.name);
                }
                if(column.hashKey){
                    hashKeys.add(column.name);
                }
            }
            options.setRangePartitionColumns(rangeKeys);
            options.addHashPartitions(hashKeys, DEFAULT_REPLICAS);
        }

/*
        PartialRow lower = getSchema().newPartialRow();
        PartialRow upper = getSchema().newPartialRow();
        lower.addInt("key", 20000);
        upper.addInt("key", 40000);

        options.addRangePartition(lower, upper);
        */
        return options;
    }

    public boolean hasNotColumns(){
        return !hasColummns();
    }
    public boolean hasColummns(){
        return (columns!=null && columns.size()>0);
    }

    public static class Builder {
        KuduTableInfo table;

        private Builder(String name) {
            table = new KuduTableInfo(name);
        }

        public static Builder create(String name) {
            return new Builder(name);
        }

        public static Builder open(String name) {
            return new Builder(name);
        }

        public Builder createIfNotExist(boolean createIfNotExist) {
            this.table.createIfNotExist = createIfNotExist;
            return this;
        }

        public Builder replicas(int replicas) {
            if (replicas == 0) return this;
            this.table.replicas = replicas;
            return this;
        }

        public Builder mode(Mode tableMode) {
            if(tableMode == null) return this;
            this.table.mode = tableMode;
            return this;
        }

        public Builder columns(List<KuduColumnInfo> columns) {
            if(columns==null) return this;
            this.table.columns.addAll(columns);
            return this;
        }

        public Builder addColumn(KuduColumnInfo column) {
            if(column==null) return this;
            this.table.columns.add(column);
            return this;
        }

        public KuduTableInfo build() {
            return table;
        }
    }
}
