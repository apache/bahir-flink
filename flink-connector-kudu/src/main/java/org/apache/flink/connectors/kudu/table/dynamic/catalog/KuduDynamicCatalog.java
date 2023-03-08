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
package org.apache.flink.connectors.kudu.table.dynamic.catalog;


import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.table.AbstractReadOnlyCatalog;
import org.apache.flink.connectors.kudu.table.dynamic.KuduDynamicTableSourceSinkFactory;
import org.apache.flink.connectors.kudu.table.utils.KuduTableUtils;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.util.StringUtils;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.AlterTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.shaded.com.google.common.collect.Lists;
import org.apache.kudu.shaded.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.flink.connectors.kudu.table.dynamic.KuduDynamicTableSourceSinkFactory.*;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Catalog for reading and creating Kudu tables.
 */
public class KuduDynamicCatalog extends AbstractReadOnlyCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(KuduDynamicCatalog.class);
    private final KuduDynamicTableSourceSinkFactory tableFactory = new KuduDynamicTableSourceSinkFactory();
    private final String kuduMasters;
    private final KuduClient kuduClient;

    /**
     * Create a new {@link KuduDynamicCatalog} with the specified catalog name and kudu master addresses.
     *
     * @param catalogName Name of the catalog (used by the table environment)
     * @param kuduMasters Connection address to Kudu
     */
    public KuduDynamicCatalog(String catalogName, String kuduMasters) {
        super(catalogName, "default_database");
        this.kuduMasters = kuduMasters;
        this.kuduClient = createClient();
    }

    /**
     * Create a new {@link KuduDynamicCatalog} with the specified kudu master addresses.
     *
     * @param kuduMasters Connection address to Kudu
     */
    public KuduDynamicCatalog(String kuduMasters) {
        this("kudu", kuduMasters);
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(getKuduTableFactory());
    }

    public KuduDynamicTableSourceSinkFactory getKuduTableFactory() {
        return tableFactory;
    }

    private KuduClient createClient() {
        return new KuduClient.KuduClientBuilder(kuduMasters).build();
    }

    @Override
    public void open() {
    }

    @Override
    public void close() {
        try {
            if (kuduClient != null) {
                kuduClient.close();
            }
        } catch (KuduException e) {
            LOG.error("Error while closing kudu client", e);
        }
    }

    public ObjectPath getObjectPath(String tableName) {
        return new ObjectPath(getDefaultDatabase(), tableName);
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");

        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        try {
            return kuduClient.getTablesList().getTablesList();
        } catch (Throwable t) {
            throw new CatalogException("Could not list tables", t);
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) {
        checkNotNull(tablePath);
        try {
            return kuduClient.tableExists(tablePath.getObjectName());
        } catch (KuduException e) {
            throw new CatalogException(e);
        }
    }

    @Override
    public CatalogTable getTable(ObjectPath tablePath) throws TableNotExistException {
        checkNotNull(tablePath);

        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }

        String tableName = tablePath.getObjectName();

        try {
            KuduTable kuduTable = kuduClient.openTable(tableName);
            // fixme base on TableSchema, TableSchema needs to be upgraded to ResolvedSchema
            CatalogTableImpl table = new CatalogTableImpl(
                    KuduTableUtils.kuduToFlinkSchema(kuduTable.getSchema()),
                    createTableProperties(tableName, kuduTable.getSchema().getPrimaryKeyColumns()),
                    tableName);

            return table;
        } catch (KuduException e) {
            throw new CatalogException(e);
        }
    }

    protected Map<String, String> createTableProperties(String tableName, List<ColumnSchema> primaryKeyColumns) {
        Map<String, String> props = new HashMap<>();
        props.put(KuduDynamicTableSourceSinkFactory.KUDU_MASTERS.key(), kuduMasters);
        String primaryKeyNames = primaryKeyColumns.stream().map(ColumnSchema::getName).collect(Collectors.joining(","));
        props.put(KUDU_PRIMARY_KEY_COLS.key(), primaryKeyNames);
        props.put(KuduDynamicTableSourceSinkFactory.KUDU_TABLE.key(), tableName);
        return props;
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) throws TableNotExistException {
        String tableName = tablePath.getObjectName();
        try {
            if (tableExists(tablePath)) {
                kuduClient.deleteTable(tableName);
            } else if (!ignoreIfNotExists) {
                throw new TableNotExistException(getName(), tablePath);
            }
        } catch (KuduException e) {
            throw new CatalogException("Could not delete table " + tableName, e);
        }
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists) throws TableNotExistException {
        String tableName = tablePath.getObjectName();
        try {
            if (tableExists(tablePath)) {
                kuduClient.alterTable(tableName, new AlterTableOptions().renameTable(newTableName));
            } else if (!ignoreIfNotExists) {
                throw new TableNotExistException(getName(), tablePath);
            }
        } catch (KuduException e) {
            throw new CatalogException("Could not rename table " + tableName, e);
        }
    }

    public void createTable(KuduTableInfo tableInfo, boolean ignoreIfExists) throws CatalogException, TableAlreadyExistException {
        ObjectPath path = getObjectPath(tableInfo.getName());
        if (tableExists(path)) {
            if (ignoreIfExists) {
                return;
            } else {
                throw new TableAlreadyExistException(getName(), path);
            }
        }

        try {
            kuduClient.createTable(tableInfo.getName(), tableInfo.getSchema(), tableInfo.getCreateTableOptions());
        } catch (
                KuduException e) {
            throw new CatalogException("Could not create table " + tableInfo.getName(), e);
        }
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) throws TableAlreadyExistException {
        Map<String, String> tableProperties = table.getOptions();
        TableSchema tableSchema = table.getSchema();

        Set<String> optionalProperties = new HashSet<>(Arrays.asList(KUDU_REPLICAS.key(),
                KUDU_HASH_PARTITION_NUMS.key(), KUDU_HASH_COLS.key()));
        Set<String> requiredProperties = new HashSet<>();

        if (!tableSchema.getPrimaryKey().isPresent()) {
            requiredProperties.add(KUDU_PRIMARY_KEY_COLS.key());
        }

        if (!tableProperties.keySet().containsAll(requiredProperties)) {
            throw new CatalogException("Missing required property. The following properties must be provided: " +
                    requiredProperties.toString());
        }

        Set<String> permittedProperties = Sets.union(requiredProperties, optionalProperties);
        if (!permittedProperties.containsAll(tableProperties.keySet())) {
            throw new CatalogException("Unpermitted properties were given. The following properties are allowed:" +
                    permittedProperties.toString());
        }

        String tableName = tablePath.getObjectName();

        KuduTableInfo tableInfo = KuduTableUtils.createTableInfo(tableName, tableSchema, tableProperties);

        createTable(tableInfo, ignoreIfExists);
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) throws CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) throws CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return Lists.newArrayList(getDefaultDatabase());
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (databaseName.equals(getDefaultDatabase())) {
            return new CatalogDatabaseImpl(new HashMap<>(), null);
        } else {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return listDatabases().contains(databaseName);
    }

    @Override
    public List<String> listViews(String databaseName) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> filters) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        return false;
    }

    @Override
    public List<String> listFunctions(String dbName) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {
        throw new FunctionNotExistException(getName(), functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return false;
    }

}
