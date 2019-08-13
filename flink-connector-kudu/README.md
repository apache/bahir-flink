# Flink Kudu Connector

This connector provides a source (```KuduInputFormat```) and a sink/output (```KuduSink``` and ```KuduOutputFormat```, respectively) that can read and write to [Kudu](https://kudu.apache.org/). To use this connector, add the
following dependency to your project:

    <dependency>
      <groupId>org.apache.bahir</groupId>
      <artifactId>flink-connector-kudu_2.11</artifactId>
      <version>1.1-SNAPSHOT</version>
    </dependency>

*Version Compatibility*: This module is compatible with Apache Kudu *1.9.0* (last stable version).

Note that the streaming connectors are not part of the binary distribution of Flink. You need to link them into your job jar for cluster execution.
See how to link with them for cluster execution [here](https://ci.apache.org/projects/flink/flink-docs-stable/start/dependencies.html).

## Installing Kudu

Follow the instructions from the [Kudu Installation Guide](https://kudu.apache.org/docs/installation.html).
Optionally, you can use the docker images provided in dockers folder. 

## KuduInputFormat

```
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

env.setParallelism(PARALLELISM);

// create a table info object
KuduTableInfo tableInfo = KuduTableInfo.Builder
        .create("books")
        .addColumn(KuduColumnInfo.Builder.createInteger("id").asKey().asHashKey().build())
        .addColumn(KuduColumnInfo.Builder.createString("title").build())
        .addColumn(KuduColumnInfo.Builder.createString("author").build())
        .addColumn(KuduColumnInfo.Builder.createDouble("price").build())
        .addColumn(KuduColumnInfo.Builder.createInteger("quantity").build())
        .build();
// create a reader configuration
KuduReaderConfig readerConfig = KuduReaderConfig.Builder
        .setMasters("172.25.0.6")
        .setRowLimit(1000)
        .build();    
// Pass the tableInfo to the KuduInputFormat and provide kuduMaster ips
env.createInput(new KuduInputFormat<>(readerConfig, tableInfo, new DefaultSerDe()))
        .count();
        
env.execute();
```

## KuduOutputFormat

```
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

env.setParallelism(PARALLELISM);

// create a table info object
KuduTableInfo tableInfo = KuduTableInfo.Builder
        .create("books")
        .createIfNotExist(true)
        .replicas(3)
        .addColumn(KuduColumnInfo.Builder.createInteger("id").asKey().asHashKey().build())
        .addColumn(KuduColumnInfo.Builder.createString("title").build())
        .addColumn(KuduColumnInfo.Builder.createString("author").build())
        .addColumn(KuduColumnInfo.Builder.createDouble("price").build())
        .addColumn(KuduColumnInfo.Builder.createInteger("quantity").build())
        .build();
// create a writer configuration
KuduWriterConfig writerConfig = KuduWriterConfig.Builder
        .setMasters("172.25.0.6")
        .setUpsertWrite()
        .setStrongConsistency()
        .build();
...

env.fromCollection(books)
        .output(new KuduOutputFormat<>(writerConfig, tableInfo, new DefaultSerDe()));

env.execute();
```

## KuduSink

```
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

env.setParallelism(PARALLELISM);

// create a table info object
KuduTableInfo tableInfo = KuduTableInfo.Builder
        .create("books")
        .createIfNotExist(true)
        .replicas(3)
        .addColumn(KuduColumnInfo.Builder.createInteger("id").asKey().asHashKey().build())
        .addColumn(KuduColumnInfo.Builder.createString("title").build())
        .addColumn(KuduColumnInfo.Builder.createString("author").build())
        .addColumn(KuduColumnInfo.Builder.createDouble("price").build())
        .addColumn(KuduColumnInfo.Builder.createInteger("quantity").build())
        .build();
// create a writer configuration
KuduWriterConfig writerConfig = KuduWriterConfig.Builder
        .setMasters("172.25.0.6")
        .setUpsertWrite()
        .setStrongConsistency()
        .build();
...

env.fromCollection(books)
    .addSink(new KuduSink<>(writerConfig, tableInfo, new DefaultSerDe()));

env.execute();
```
