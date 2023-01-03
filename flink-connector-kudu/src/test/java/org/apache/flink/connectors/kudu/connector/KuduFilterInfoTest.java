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
package org.apache.flink.connectors.kudu.connector;

import org.apache.flink.table.data.binary.BinaryStringData;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class KuduFilterInfoTest {

    @Test
    void testKuduFilterInfoWithBinaryStringData() {
        String filterValue = "someValue";

        KuduFilterInfo kuduFilterInfo = KuduFilterInfo.Builder.create("col")
                .equalTo(BinaryStringData.fromString(filterValue))
                .build();

        ColumnSchema colSchema = new ColumnSchema.ColumnSchemaBuilder("col", Type.STRING).build();
        assertDoesNotThrow(() -> kuduFilterInfo.toPredicate(colSchema));
    }

}
