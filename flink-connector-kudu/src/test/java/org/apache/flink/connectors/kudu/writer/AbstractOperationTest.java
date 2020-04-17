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
package org.apache.flink.connectors.kudu.writer;

import org.apache.flink.connectors.kudu.connector.KuduTestBase;

import org.apache.kudu.Schema;
import org.apache.kudu.client.Delete;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Update;
import org.apache.kudu.client.Upsert;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.when;

public abstract class AbstractOperationTest {

    public static final Schema tableSchema = KuduTestBase.booksTableInfo("test_table", true).getSchema();
    @Mock
    Insert mockInsert;
    @Mock
    Upsert mockUpsert;
    @Mock
    Update mockUpdate;
    @Mock
    Delete mockDelete;
    @Mock
    KuduTable mockTable;
    @Mock
    PartialRow mockPartialRow;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.initMocks(this);
        when(mockInsert.getRow()).thenReturn(mockPartialRow);
        when(mockUpsert.getRow()).thenReturn(mockPartialRow);
        when(mockUpdate.getRow()).thenReturn(mockPartialRow);
        when(mockDelete.getRow()).thenReturn(mockPartialRow);
        when(mockTable.newInsert()).thenReturn(mockInsert);
        when(mockTable.newUpsert()).thenReturn(mockUpsert);
        when(mockTable.newUpdate()).thenReturn(mockUpdate);
        when(mockTable.newDelete()).thenReturn(mockDelete);
        when(mockTable.getSchema()).thenReturn(tableSchema);
    }
}
