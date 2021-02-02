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

import org.apache.commons.lang3.Validate;
import org.apache.kudu.Schema;
import org.apache.kudu.client.CreateTableOptions;

import java.io.Serializable;
import java.util.Objects;

/**
 * Describes which table should be used in sources and sinks along with specifications
 * on how to create it if it does not exist.
 *
 * <p> For sources and sinks reading from already existing tables, simply use @{@link KuduTableInfo#forTable(String)}
 * and if you want the system to create the table if it does not exist you need to specify the column and options
 * factories through {@link KuduTableInfo#createTableIfNotExists}
 */
@PublicEvolving
public class KuduTableInfo implements Serializable {

    private String name;
    private CreateTableOptionsFactory createTableOptionsFactory = null;
    private ColumnSchemasFactory schemasFactory = null;

    private KuduTableInfo(String name) {
        this.name = Validate.notNull(name);
    }

    /**
     * Creates a new {@link KuduTableInfo} that is sufficient for reading/writing to existing Kudu Tables.
     * For creating new tables call {@link #createTableIfNotExists} afterwards.
     *
     * @param name Table name in Kudu
     * @return KuduTableInfo for the given table name
     */
    public static KuduTableInfo forTable(String name) {
        return new KuduTableInfo(name);
    }

    /**
     * Defines table parameters to be used when creating the Kudu table if it does not exist (read or write)
     *
     * @param schemasFactory            factory for defining columns
     * @param createTableOptionsFactory factory for defining create table options
     * @return KuduTableInfo that will create tables that does not exist with the given settings.
     */
    public KuduTableInfo createTableIfNotExists(ColumnSchemasFactory schemasFactory, CreateTableOptionsFactory createTableOptionsFactory) {
        this.createTableOptionsFactory = Validate.notNull(createTableOptionsFactory);
        this.schemasFactory = Validate.notNull(schemasFactory);
        return this;
    }

    /**
     * Returns the {@link Schema} of the table. Only works if {@link #createTableIfNotExists} was specified otherwise throws an error.
     *
     * @return Schema of the target table.
     */
    public Schema getSchema() {
        if (!getCreateTableIfNotExists()) {
            throw new RuntimeException("Cannot access schema for KuduTableInfo. Use createTableIfNotExists to specify the columns.");
        }

        return new Schema(schemasFactory.getColumnSchemas());
    }

    /**
     * @return Name of the table.
     */
    public String getName() {
        return name;
    }

    /**
     * @return True if table creation is enabled if target table does not exist.
     */
    public boolean getCreateTableIfNotExists() {
        return createTableOptionsFactory != null;
    }

    /**
     * @return CreateTableOptions if {@link #createTableIfNotExists} was specified.
     */
    public CreateTableOptions getCreateTableOptions() {
        if (!getCreateTableIfNotExists()) {
            throw new RuntimeException("Cannot access CreateTableOptions for KuduTableInfo. Use createTableIfNotExists to specify.");
        }
        return createTableOptionsFactory.getCreateTableOptions();
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KuduTableInfo that = (KuduTableInfo) o;
        return Objects.equals(this.name, that.name);
    }
}
