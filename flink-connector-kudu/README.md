# Flink Kudu Connector

This connector provides a source (```KuduInputFormat```), a sink/output
(```KuduSink``` and ```KuduOutputFormat```, respectively),
 as well a table source (`KuduTableSource`), an upsert table sink (`KuduTableSink`), and a catalog (`KuduCatalog`),
 to allow reading and writing to [Kudu](https://kudu.apache.org/).

To use this connector, add the following dependency to your project:

    <dependency>
      <groupId>org.apache.bahir</groupId>
      <artifactId>flink-connector-kudu_2.11</artifactId>
      <version>1.1-SNAPSHOT</version>
    </dependency>

 *Version Compatibility*: This module is compatible with Apache Kudu *1.11.1* (last stable version) and Apache Flink 1.10.+.

Note that the streaming connectors are not part of the binary distribution of Flink. You need to link them into your job jar for cluster execution.
See how to link with them for cluster execution [here](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/projectsetup/dependencies.html).

## Installing Kudu

Follow the instructions from the [Kudu Installation Guide](https://kudu.apache.org/docs/installation.html).
Optionally, you can use the docker images provided in dockers folder.

## SQL and Table API

The Kudu connector is fully integrated with the Flink Table and SQL APIs. Once we configure the Kudu catalog (see next section)
we can start querying or inserting into existing Kudu tables using the Flink SQL or Table API.

For more information about the possible queries please check the [official documentation](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/sql/)

### Kudu Catalog

The connector comes with a catalog implementation to handle metadata about your Kudu setup and perform table management.
By using the Kudu catalog, you can access all the tables already created in Kudu from Flink SQL queries. The Kudu catalog only
allows users to create or access existing Kudu tables. Tables using other data sources must be defined in other catalogs such as
in-memory catalog or Hive catalog.

When using the SQL CLI you can easily add the Kudu catalog to your environment yaml file:

```
catalogs:
  - name: kudu
    type: kudu
    kudu.masters: <host>:7051
```

Once the SQL CLI is started you can simply switch to the Kudu catalog by calling `USE CATALOG kudu;`

You can also create and use the KuduCatalog directly in the Table environment:

```java
String KUDU_MASTERS="host1:port1,host2:port2"
KuduCatalog catalog = new KuduCatalog(KUDU_MASTERS);
tableEnv.registerCatalog("kudu", catalog);
tableEnv.useCatalog("kudu");
```

### DDL operations using SQL

It is possible to manipulate Kudu tables using SQL DDL.

When not using the Kudu catalog, the following additional properties must be specified in the `WITH` clause:
* `'connector.type'='kudu'`
* `'kudu.masters'='host1:port1,host2:port2,...'`: comma-delimitered list of Kudu masters
* `'kudu.table'='...'`: The table's name within the Kudu database.

If you have registered and are using the Kudu catalog, these properties are handled automatically.

To create a table, the additional properties `kudu.primary-key-columns` and `kudu.hash-columns` must be specified
as comma-delimited lists. Optionally, you can set the `kudu.replicas` property (defaults to 1).
Other properties, such as range partitioning, cannot be configured here - for more flexibility, please use
`catalog.createTable` as described in [this](#Creating-a-KuduTable-directly-with-KuduCatalog) section or create the table directly in Kudu.

The `NOT NULL` constraint can be added to any of the column definitions.
By setting a column as a primary key, it will automatically by created with the `NOT NULL` constraint.
Hash columns must be a subset of primary key columns.

Kudu Catalog

```
CREATE TABLE TestTable (
  first STRING,
  second STRING,
  third INT NOT NULL
) WITH (
  'kudu.hash-columns' = 'first',
  'kudu.primary-key-columns' = 'first,second'
)
```

Other catalogs
```
CREATE TABLE TestTable (
  first STRING,
  second STRING,
  third INT NOT NULL
) WITH (
  'connector.type' = 'kudu',
  'kudu.masters' = '...',
  'kudu.table' = 'TestTable',
  'kudu.hash-columns' = 'first',
  'kudu.primary-key-columns' = 'first,second'
)
```

Renaming a table:
```
ALTER TABLE TestTable RENAME TO TestTableRen
```

Dropping a table:
```sql
DROP TABLE TestTableRen
```

#### Creating a KuduTable directly with KuduCatalog

The KuduCatalog also exposes a simple `createTable` method that required only the where table configuration,
including schema, partitioning, replication, etc. can be specified using a `KuduTableInfo` object.

Use the `createTableIfNotExists` method, that takes a `ColumnSchemasFactory` and
a `CreateTableOptionsFactory` parameter, that implement respectively `getColumnSchemas()`
returning a list of Kudu [ColumnSchema](https://kudu.apache.org/apidocs/org/apache/kudu/ColumnSchema.html) objects;
 and  `getCreateTableOptions()` returning a
[CreateTableOptions](https://kudu.apache.org/apidocs/org/apache/kudu/client/CreateTableOptions.html) object.

This example shows the creation of a table called `ExampleTable` with two columns,
`first` being a primary key; and configuration of replicas and hash partitioning.

```java
KuduTableInfo tableInfo = KuduTableInfo
    .forTable("ExampleTable")
    .createTableIfNotExists(
        () ->
            Lists.newArrayList(
                new ColumnSchema
                    .ColumnSchemaBuilder("first", Type.INT32)
                    .key(true)
                    .build(),
                new ColumnSchema
                    .ColumnSchemaBuilder("second", Type.STRING)
                    .build()
            ),
        () -> new CreateTableOptions()
            .setNumReplicas(1)
            .addHashPartitions(Lists.newArrayList("first"), 2));

catalog.createTable(tableInfo, false);
```
The example uses lambda expressions to implement the functional interfaces.

Read more about Kudu schema design in the [Kudu docs](https://kudu.apache.org/docs/schema_design.html).

### Supported data types

| Flink/SQL            | Kudu                    |
|----------------------|:-----------------------:|
| `STRING`             | STRING                  |
| `BOOLEAN`            | BOOL                    |
| `TINYINT`            | INT8                    |
| `SMALLINT`           | INT16                   |
| `INT`                | INT32                   |
| `BIGINT`             | INT64                   |
| `FLOAT`              | FLOAT                   |
| `DOUBLE`             | DOUBLE                  |
| `BYTES`              | BINARY                  |
| `TIMESTAMP(3)`       | UNIXTIME_MICROS         |

Note:
* `TIMESTAMP`s are fixed to a precision of 3, and the corresponding Java conversion class is `java.sql.Timestamp` 
* `BINARY` and `VARBINARY` are not yet supported - use `BYTES`, which is a `VARBINARY(2147483647)`
*  `CHAR` and `VARCHAR` are not yet supported - use `STRING`, which is a `VARCHAR(2147483647)`
* `DECIMAL` types are not yet supported

### Known limitations
* Data type limitations (see above).
* SQL Create table: primary keys can only be set by the `kudu.primary-key-columns` property, using the
`PRIMARY KEY` constraint is not yet possible.
* SQL Create table: range partitioning is not supported.
* When getting a table through the Catalog, NOT NULL and PRIMARY KEY constraints are ignored. All columns
are described as being nullable, and not being primary keys.
* Kudu tables cannot be altered through the catalog other than simple renaming

## DataStream API

It is also possible to use the Kudu connector directly from the DataStream API however we
encourage all users to explore the Table API as it provides a lot of useful tooling when working
with Kudu data.

### Reading tables into a DataStreams

There are 2 main ways of reading a Kudu Table into a DataStream
 1. Using the `KuduCatalog` and the Table API
 2. Using the `KuduRowInputFormat` directly

Using the `KuduCatalog` and Table API is the recommended way of reading tables as it automatically
guarantees type safety and takes care of configuration of our readers.

This is how it works in practice:
```java
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, tableSettings);

tableEnv.registerCatalog("kudu", new KuduCatalog("master:port"));
tableEnv.useCatalog("kudu");

Table table = tableEnv.sqlQuery("SELECT * FROM MyKuduTable");
DataStream<Row> rows = tableEnv.toAppendStream(table, Row.class);
```

The second way of achieving the same thing is by using the `KuduRowInputFormat` directly.
In this case we have to manually provide all information about our table:

```java
KuduTableInfo tableInfo = ...
KuduReaderConfig readerConfig = ...
KuduRowInputFormat inputFormat = new KuduRowInputFormat(readerConfig, tableInfo);

DataStream<Row> rowStream = env.createInput(inputFormat, rowTypeInfo);
```

At the end of the day the `KuduTableSource` is just a convenient wrapper around the `KuduRowInputFormat`.

### Kudu Sink
The connector provides a `KuduSink` class that can be used to consume DataStreams
and write the results into a Kudu table.

The constructor takes 3 or 4 arguments.
 * `KuduWriterConfig` is used to specify the Kudu masters and the flush mode.
 * `KuduTableInfo` identifies the table to be written
 * `KuduOperationMapper` maps the records coming from the DataStream to a list of Kudu operations.
 * `KuduFailureHandler` (optional): If you want to provide your own logic for handling writing failures.

The example below shows the creation of a sink for Row type records of 3 fields. It Upserts each record.
It is assumed that a Kudu table with columns `col1, col2, col3` called `AlreadyExistingTable` exists. Note that if this were not the case,
we could pass a `KuduTableInfo` as described in the [Catalog - Creating a table](#creating-a-table) section,
and the sink would create the table with the provided configuration.

```java
KuduWriterConfig writerConfig = KuduWriterConfig.Builder.setMasters(KUDU_MASTERS).build();

KuduSink<Row> sink = new KuduSink<>(
    writerConfig,
    KuduTableInfo.forTable("AlreadyExistingTable"),
    new RowOperationMapper<>(
            new String[]{"col1", "col2", "col3"},
            AbstractSingleOperationMapper.KuduOperation.UPSERT)
)
```

#### KuduOperationMapper

This section describes the Operation mapping logic in more detail.

The connector supports insert, upsert, update, and delete operations.
The operation to be performed can vary dynamically based on the record.
To allow for more flexibility, it is also possible for one record to trigger
0, 1, or more operations.
For the highest level of control, implement the `KuduOperationMapper` interface.

If one record from the DataStream corresponds to one table operation,
extend the `AbstractSingleOperationMapper` class. An array of column
names must be provided. This must match the Kudu table's schema.

The `getField` method must be overridden, which extracts the value for the table column whose name is
at the `i`th place in the `columnNames` array.
If the operation is one of (`CREATE, UPSERT, UPDATE, DELETE`)
and doesn't depend on the input record (constant during the life of the sink), it can be set in the constructor
of `AbstractSingleOperationMapper`.
It is also possible to implement your own logic by overriding the
`createBaseOperation` method that returns a Kudu [Operation](https://kudu.apache.org/apidocs/org/apache/kudu/client/Operation.html).

There are pre-defined operation mappers for Pojo, Flink Row, and Flink Tuple types for constant operation, 1-to-1 sinks.
* `PojoOperationMapper`: Each table column must correspond to a POJO field
with the same name. The  `columnNames` array should contain those fields of the POJO that
are present as table columns (the POJO fields can be a superset of table columns).
* `RowOperationMapper` and `TupleOperationMapper`: the mapping is based on position. The
`i`th field of the Row/Tuple corresponds to the column of the table at the `i`th
position in the `columnNames` array.

## Building the connector

The connector can be easily built by using maven:

```
cd bahir-flink
mvn clean install
```

### Running the tests

The integration tests rely on the Kudu test harness which requires the current user to be able to ssh to localhost.

This might not work out of the box on some operating systems (such as Mac OS X).
To solve this problem go to *System Preferences/Sharing* and enable Remote login for your user.
