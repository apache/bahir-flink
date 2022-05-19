/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.kudu.table.function.lookup;

import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.convertor.RowResultRowDataConvertor;
import org.apache.flink.connectors.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

/**
 * LookupFunction based on the RowData object type
 */
public class KuduRowDataLookupFunction extends AbstractKuduLookupFunction<RowData> {
    private static final long serialVersionUID = 1L;


    private KuduRowDataLookupFunction(String[] keyNames, KuduTableInfo tableInfo, KuduReaderConfig kuduReaderConfig, String[] projectedFields, KuduLookupOptions kuduLookupOptions) {
        super(keyNames, new RowResultRowDataConvertor(), tableInfo, kuduReaderConfig, projectedFields, kuduLookupOptions);
    }

    @Override
    public RowData buildCacheKey(Object... keys) {
        return GenericRowData.of(keys);
    }

    public static class Builder {
        private KuduTableInfo tableInfo;
        private KuduReaderConfig kuduReaderConfig;
        private String[] keyNames;
        private String[] projectedFields;
        private KuduLookupOptions kuduLookupOptions;

        public static Builder options() {
            return new Builder();
        }

        public Builder tableInfo(KuduTableInfo tableInfo) {
            this.tableInfo = tableInfo;
            return this;
        }

        public Builder kuduReaderConfig(KuduReaderConfig kuduReaderConfig) {
            this.kuduReaderConfig = kuduReaderConfig;
            return this;
        }

        public Builder keyNames(String[] keyNames) {
            this.keyNames = keyNames;
            return this;
        }

        public Builder projectedFields(String[] projectedFields) {
            this.projectedFields = projectedFields;
            return this;
        }

        public Builder kuduLookupOptions(KuduLookupOptions kuduLookupOptions) {
            this.kuduLookupOptions = kuduLookupOptions;
            return this;
        }

        public KuduRowDataLookupFunction build() {
            return new KuduRowDataLookupFunction(keyNames, tableInfo, kuduReaderConfig, projectedFields, kuduLookupOptions);
        }
    }
}