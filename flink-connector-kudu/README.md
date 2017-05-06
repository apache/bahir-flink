# Flink Kudu Connector
This connector provides a source and sink to [Apache Kudu](http://kudu.apache.org/)™
To use this connector, add the following dependency to your project:

```
<dependency>
  <groupId>es.accenture</groupId>
  <artifactId>flink-kudu-connector</artifactId>
  <version>1.0</version>
</dependency>
```


Data flows patterns:
* Batch
  * Kudu -> DataSet\<RowSerializable\> -> Kudu
  * Kudu -> DataSet\<RowSerializable\> -> other source
  * Other source -> DataSet\<RowSerializable\> -> other source
* Stream
  * Other source -> DataStream \<RowSerializable\> -> Kudu


```java

/* Batch mode - DataSet API -*/

DataSet<RowSerializable> input = KuduInputBuilder.build(TABLE_SOURCE, KUDU_MASTER)
               
// DataSet operations --> .map(), .filter(), reduce(), etc.
//result = input.map(...)

result.output(new KuduOutputFormat(KUDU_MASTER, TABLE_SINK, columnNames, KuduOutputFormat.CREATE));

KuduInputBuilder.env.execute();

```

```java

/* Streaming mode - DataSream API - */
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStream<String> stream = env.fromElements("data1 data2 data3");
DataStream<RowSerializable> stream2 = stream.map(new MapToRowSerializable());

stream2.addSink(new KuduSink(KUDU_MASTER, DEST_TABLE, columnNames));
env.execute();


```


## Requirements

* Flink and Kudu compatible OS
* Java (version 8)
* Scala (version 2.12.1)
* Apache Flink (version 1.1.3)
* Apache Kudu (version 1.2.0)
* Apache Kafka (version 0.10.1.1) (only for streaming example)
* Maven (version 3.3.9) (only for building)



## Execution

Start Flink Job Manager
```
<Flink-installation-folder>/bin/start-local.sh
```
Submit the job
```
<Flink-instalation-folder>/bin/flink run -c <Job-package-path> target/flink-kudu-1.0-SNAPSHOT.jar param1 param2 ...
```

