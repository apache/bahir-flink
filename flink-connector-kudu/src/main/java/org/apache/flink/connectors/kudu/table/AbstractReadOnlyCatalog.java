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

package org.apache.flink.connectors.kudu.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;

/**
 * Read-only catalog.
 */
@Internal
public abstract class AbstractReadOnlyCatalog extends AbstractCatalog {

    private static final CatalogException UNSUPPORTED_ERR = new CatalogException("This action is not supported for read-only catalogs");

    public AbstractReadOnlyCatalog(String name, String defaultDatabase) {
        super(name, defaultDatabase);
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists) throws DatabaseAlreadyExistException, CatalogException {
        throw UNSUPPORTED_ERR;
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade) throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        throw UNSUPPORTED_ERR;
    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists) throws DatabaseNotExistException, CatalogException {
        throw UNSUPPORTED_ERR;
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) throws TableAlreadyExistException {
        throw UNSUPPORTED_ERR;
    }

    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists) throws TableNotExistException {
        throw UNSUPPORTED_ERR;
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
        throw UNSUPPORTED_ERR;
    }

    @Override
    public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition partition, boolean ignoreIfExists) throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, PartitionAlreadyExistsException, CatalogException {
        throw UNSUPPORTED_ERR;
    }

    @Override
    public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
        throw UNSUPPORTED_ERR;
    }

    @Override
    public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition newPartition, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
        throw UNSUPPORTED_ERR;
    }

    @Override
    public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists) throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw UNSUPPORTED_ERR;
    }

    @Override
    public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {
        throw UNSUPPORTED_ERR;
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {
        throw UNSUPPORTED_ERR;
    }

    @Override
    public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        throw UNSUPPORTED_ERR;
    }

    @Override
    public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException, TablePartitionedException {
        throw UNSUPPORTED_ERR;
    }

    @Override
    public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogTableStatistics partitionStatistics, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
        throw UNSUPPORTED_ERR;
    }

    @Override
    public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
        throw UNSUPPORTED_ERR;
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) throws TableNotExistException {
        throw UNSUPPORTED_ERR;
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists) throws TableNotExistException {
        throw UNSUPPORTED_ERR;
    }
}
