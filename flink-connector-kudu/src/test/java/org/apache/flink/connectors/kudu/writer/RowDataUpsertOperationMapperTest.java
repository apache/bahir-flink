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
import org.apache.flink.connectors.kudu.connector.writer.RowDataUpsertOperationMapper;
import org.apache.flink.table.data.RowData;
import org.apache.kudu.client.Operation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;

/**
 * Unit Tests for {@link RowDataUpsertOperationMapper}.
 */
public class RowDataUpsertOperationMapperTest extends AbstractOperationTest {

    @Test
    void testGetField() {
        RowDataUpsertOperationMapper mapper =
                new RowDataUpsertOperationMapper(KuduTestBase.booksTableSchema());
        RowData inputRow = KuduTestBase.booksRowData().get(0);

        Assertions.assertEquals(inputRow.getInt(0), mapper.getField(inputRow, 0));
    }


    @Test
    void testCorrectOperationUpsert() {
        RowDataUpsertOperationMapper mapper =
                new RowDataUpsertOperationMapper(KuduTestBase.booksTableSchema());
        RowData inputRow = KuduTestBase.booksRowData().get(0);

        List<Operation> operations = mapper.createOperations(inputRow, mockTable);

        assertEquals(1, operations.size());
        verify(mockTable).newUpsert();
    }
}