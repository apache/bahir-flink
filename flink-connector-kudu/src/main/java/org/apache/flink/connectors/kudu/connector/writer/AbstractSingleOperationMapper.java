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
package org.apache.flink.connectors.kudu.connector.writer;

import org.apache.flink.annotation.PublicEvolving;

import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Base implementation for {@link KuduOperationMapper}s that have one-to-one input to
 * Kudu operation mapping. It requires a fixed table schema to be provided at construction
 * time and only requires users to implement a getter for a specific column index (relative
 * to the ones provided in the constructor).
 * <br>
 * Supports both fixed operation type per record by specifying the {@link KuduOperation} or a
 * custom implementation for creating the base {@link Operation} throwugh the
 * {@link #createBaseOperation(Object, KuduTable)} method.
 *
 * @param <T> Input type
 */
@PublicEvolving
public abstract class AbstractSingleOperationMapper<T> implements KuduOperationMapper<T> {

    protected final String[] columnNames;
    private final KuduOperation operation;

    protected AbstractSingleOperationMapper(String[] columnNames) {
        this(columnNames, null);
    }

    public AbstractSingleOperationMapper(String[] columnNames, KuduOperation operation) {
        this.columnNames = columnNames;
        this.operation = operation;
    }

    /**
     * Returns the object corresponding to the given column index.
     *
     * @param input Input element
     * @param i     Column index
     * @return Column value
     */
    public abstract Object getField(T input, int i);

    public Optional<Operation> createBaseOperation(T input, KuduTable table) {
        if (operation == null) {
            throw new UnsupportedOperationException("createBaseOperation must be overridden if no operation specified in constructor");
        }
        switch (operation) {
            case INSERT:
                return Optional.of(table.newInsert());
            case UPDATE:
                return Optional.of(table.newUpdate());
            case UPSERT:
                return Optional.of(table.newUpsert());
            case DELETE:
                return Optional.of(table.newDelete());
            default:
                throw new RuntimeException("Unknown operation " + operation);
        }
    }

    @Override
    public List<Operation> createOperations(T input, KuduTable table) {
        Optional<Operation> operationOpt = createBaseOperation(input, table);
        if (!operationOpt.isPresent()) {
            return Collections.emptyList();
        }

        Operation operation = operationOpt.get();
        PartialRow partialRow = operation.getRow();

        for (int i = 0; i < columnNames.length; i++) {
            partialRow.addObject(columnNames[i], getField(input, i));
        }

        return Collections.singletonList(operation);
    }

    public enum KuduOperation {
        INSERT,
        UPDATE,
        UPSERT,
        DELETE
    }
}
