package org.apache.flink.streaming.connectors.redis;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.descriptor.Redis;
import org.apache.flink.streaming.connectors.redis.descriptor.RedisVadidator;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;

public class RedisDescriptorTest extends  RedisITCaseBase{

    private static final String REDIS_KEY = "TEST_KEY";

    StreamExecutionEnvironment env;

    @Before
    public void setUp(){
        env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    @Test
    public void testRedisDescriptor() throws Exception {
        DataStreamSource<Row> source = (DataStreamSource<Row>) env.addSource(new TestSourceFunctionString())
                .returns(new RowTypeInfo(TypeInformation.of(String.class), TypeInformation.of(Long.class)));

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
        tableEnvironment.registerDataStream("t1", source, "k, v");

        Redis redis = new Redis()
                .mode(RedisVadidator.REDIS_CLUSTER)
                .command(RedisCommand.INCRBY_EX.name())
                .ttl(100000)
                .property(RedisVadidator.REDIS_NODES, REDIS_HOST+ ":" + REDIS_PORT);

        tableEnvironment
                .connect(redis).withSchema(new Schema()
                .field("k", TypeInformation.of(String.class))
                .field("v", TypeInformation.of(Long.class)))
                .registerTableSink("redis");


        tableEnvironment.sqlUpdate("insert into redis select k, v from t1");
        env.execute("Test Redis Table");
    }


    private static class TestSourceFunctionString implements SourceFunction<Row> {
        private static final long serialVersionUID = 1L;

        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Row> ctx) throws Exception {
            while (running) {
                Row row = new Row(2);
                row.setField(0, REDIS_KEY);
                row.setField(1, 2L);
                ctx.collect(row);
                Thread.sleep(2000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

}
