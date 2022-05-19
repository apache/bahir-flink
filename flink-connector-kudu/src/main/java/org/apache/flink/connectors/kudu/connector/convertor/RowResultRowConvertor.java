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
package org.apache.flink.connectors.kudu.connector.convertor;

import org.apache.flink.annotation.Internal;
import org.apache.flink.types.Row;
import org.apache.kudu.Schema;
import org.apache.kudu.client.RowResult;

/**
 * Transform the Kudu RowResult object into a Flink Row object
 */
@Internal
public class RowResultRowConvertor implements RowResultConvertor<Row> {
    @Override
    public Row convertor(RowResult row) {
        Schema schema = row.getColumnProjection();

        Row values = new Row(schema.getColumnCount());
        schema.getColumns().forEach(column -> {
            String name = column.getName();
            int pos = schema.getColumnIndex(name);
            if (row.isNull(name)) {
                return;
            }
            values.setField(pos, row.getObject(name));
        });
        return values;
    }
}