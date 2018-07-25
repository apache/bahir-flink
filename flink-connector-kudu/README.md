# Flink Kudu Connector

This connector provides a source (```KuduInputFormat```) and a sink/output (```KuduSink``` and ```KuduOutputFormat```, respectively) that can read and write to [Kudu](https://kudu.apache.org/). To use this connector, add the
following dependency to your project:

    <dependency>
      <groupId>org.apache.bahir</groupId>
      <artifactId>flink-connector-kudu_2.11</artifactId>
      <version>1.1-SNAPSHOT</version>
    </dependency>

*Version Compatibility*: This module is compatible with Apache Kudu *1.5.0-cdh5.13.0* (last stable version).

Note that the streaming connectors are not part of the binary distribution of Flink. You need to link them into your job jar for cluster execution.
See how to link with them for cluster execution [here](https://ci.apache.org/projects/flink/flink-docs-release-1.3/dev/linking.html).

## Installing Kudu

Follow the instructions from the[Kudu download page](https://www.cloudera.com/documentation/kudu/latest/topics/kudu_installation.html).
Optionally, you can download a quickstart virtual machine from[here](https://kudu.apache.org/docs/quickstart.html). It provides a linux-based OS with Kudu and[Impala](https://impala.apache.org/) already installed. 

## KuduInputFormat

```
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(PARALLELISM);

        // create a configuration object
        KuduInputFormat.Conf inputConfig = KuduInputFormat.Conf
            .builder()
            .masterAddress("quickstart.cloudera")
            .tableName("impala::default.sfmta")
            .addPredicate(Predicate.columnByName("vehicle_tag").isEqualTo().val(24))
            .addPredicate(Predicate.columnByIndex(0).isLowerOrEqual().val(1356998410))
            .project("report_time", "vehicle_tag", "longitude", "latitude", "speed")
            .build();
            
        // Pass the configuration to the KuduInputFormat and provide the tuple-based type information (according to your projection)
        env.createInput(new KuduInputFormat<>(inputConfig), new TupleTypeInfo<Tuple6<Long, Integer, Float, Float, Float, Float>>(
            BasicTypeInfo.LONG_TYPE_INFO,
            BasicTypeInfo.INT_TYPE_INFO,
            BasicTypeInfo.FLOAT_TYPE_INFO,
            BasicTypeInfo.FLOAT_TYPE_INFO,
            BasicTypeInfo.FLOAT_TYPE_INFO,
            BasicTypeInfo.FLOAT_TYPE_INFO
          )
        )
        .count();
```

Note that you can specify two different write modes: `ÌNSERT` or `UPSERT`. If we try to insert an existing record with the `INSERT mode, the driver just ignores the record.

## KuduOutputFormat

```
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(PARALLELISM);
        
        KuduOutputFormat.Conf outputConfig = KuduOutputFormat.Conf
            .builder()
            .masterAddress("quickstart.cloudera")
            .tableName("impala::default.test")
            .writeMode(UPSERT)
            .build();

        List<Tuple3<String, String, Boolean>> list = new ArrayList<>();

        for(int i = 0; i < 2000; i++){
            list.add(new Tuple3<>(
                    UUID.randomUUID().toString(),
                    UUID.randomUUID().toString(),
                    new Random().nextBoolean())
            );
        }

        env
            .fromCollection(list)
            .output(new KuduOutputFormat<>(outputConfig));

        env.execute();
```

## KuduSink

```
        KuduOutputFormat.Conf outputConfig = KuduOutputFormat.Conf
                .builder()
                .masterAddress("quickstart.cloudera")
                .tableName("impala::default.test")
                .writeMode(UPSERT)
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(PARALLELISM);

        List<Tuple3<String, String, Boolean>> list = new ArrayList<>();

        for (int i = 0; i < 2000; i++) {
            list.add(new Tuple3<>(
                    UUID.randomUUID().toString(),
                    UUID.randomUUID().toString(),
                    new Random().nextBoolean())
            );
        }

        env
            .fromCollection(list)
            .addSink(new KuduSink<>(outputConfig));

        env.execute();
```
     


## Current Limitations

* `limit()` operator is not implemented in the Kudu Scanner API, so limit() clauses in your queries will be ignored. See https://issues.apache.org/jira/browse/KUDU-16.

```
      KuduInputFormat.Conf inputConfig = KuduInputFormat.Conf
        .builder()
        .masterAddress("quickstart.cloudera")
        .tableName("impala::default.sfmta")
        .limit(3) // will be ignored
        .build();

```