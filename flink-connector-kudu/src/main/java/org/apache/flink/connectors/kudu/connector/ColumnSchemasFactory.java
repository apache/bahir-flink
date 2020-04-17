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

import org.apache.flink.annotation.PublicEvolving;

import org.apache.kudu.ColumnSchema;

import java.io.Serializable;
import java.util.List;

/**
 * Factory for creating {@link ColumnSchema}s to be used when creating a table that
 * does not currently exist in Kudu. Usable through {@link KuduTableInfo#createTableIfNotExists}.
 *
 * <p> This factory implementation must be Serializable as it will be used directly in the Flink sources
 * and sinks.
 */
@PublicEvolving
public interface ColumnSchemasFactory extends Serializable {

    /**
     * Creates the columns of the Kudu table that will be used during the createTable operation.
     *
     * @return List of columns.
     */
    List<ColumnSchema> getColumnSchemas();

}
