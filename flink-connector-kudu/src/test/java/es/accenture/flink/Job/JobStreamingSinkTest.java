package es.accenture.flink.Job;

import es.accenture.flink.Sink.KuduSink;
import es.accenture.flink.Utils.RowSerializable;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kudu.client.KuduClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class JobStreamingSinkTest {


    private String KUDU_MASTER;
    private String TABLE_NAME;
    private String [] columnNames = new String[2];
    private KuduClient client;
    private StreamExecutionEnvironment env;

    @Before
    public void setUp() throws Exception {

        TABLE_NAME = "TableSink";
        KUDU_MASTER = "localhost";
        columnNames[0] = "key";
        columnNames[1] = "value";

        client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = env.fromElements("field1 field2");
        DataStream<RowSerializable> stream2 = stream.map(new TestUtils.MyMapFunction2());
        stream2.addSink(new KuduSink(KUDU_MASTER, TABLE_NAME, columnNames));

    }

    @After
    public void tearDown() throws Exception {
       if(client.tableExists(TABLE_NAME))
           client.deleteTable(TABLE_NAME);
    }


    @Test
    public void JobStreamingSinkTest() throws Exception {

        setUp();

        if(client.tableExists(TABLE_NAME))
            client.deleteTable(TABLE_NAME);
        assert !client.tableExists(TABLE_NAME): "JUnit error: Table already exists";
        assert TestUtils.createTable(client,TABLE_NAME, "STRING");
        env.execute();
        assert TestUtils.numRows(client,TABLE_NAME) == 1;
        assert TestUtils.scanRows(client, TABLE_NAME).contains("STRING key=FIELD1, STRING value=FIELD2"):"JUnit error: row not found";


    }

}
