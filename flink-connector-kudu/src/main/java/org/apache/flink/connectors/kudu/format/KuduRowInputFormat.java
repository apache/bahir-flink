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
package org.apache.flink.connectors.kudu.format;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connectors.kudu.connector.KuduFilterInfo;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.convertor.RowResultConvertor;
import org.apache.flink.connectors.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.types.Row;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * InputFormat based on the row object type
 */
@PublicEvolving
public class KuduRowInputFormat extends AbstractKuduInputFormat<Row> {

    public KuduRowInputFormat(KuduReaderConfig readerConfig, RowResultConvertor<Row> rowResultConvertor, KuduTableInfo tableInfo) {
        super(readerConfig, rowResultConvertor, tableInfo);
    }

    public KuduRowInputFormat(KuduReaderConfig readerConfig, RowResultConvertor<Row> rowResultConvertor, KuduTableInfo tableInfo, List<String> tableProjections) {
        super(readerConfig, rowResultConvertor, tableInfo, tableProjections);
    }

    public KuduRowInputFormat(KuduReaderConfig readerConfig, RowResultConvertor<Row> rowResultConvertor, KuduTableInfo tableInfo, List<KuduFilterInfo> tableFilters, List<String> tableProjections) {
        super(readerConfig, rowResultConvertor, tableInfo, tableFilters, tableProjections);
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return TypeInformation.of(Row.class);
    }
}