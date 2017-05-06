package es.accenture.flink.Job;

import es.accenture.flink.Sink.KuduOutputFormat;
import es.accenture.flink.Sources.KuduInputFormat;
import es.accenture.flink.Utils.Exceptions.KuduClientException;
import es.accenture.flink.Utils.Exceptions.KuduTableException;
import es.accenture.flink.Utils.RowSerializable;
import es.accenture.flink.Utils.Utils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class JobBatchSinkTest {

    /*class vars*/
    private String KUDU_MASTER;
    private String TABLE_NAME;
    private String TABLE_NAME2;
    private String[] columnNames;
    private Utils utils;
    private KuduClient client;
    private ExecutionEnvironment env;
    private boolean singleton = true;
    private DataSet<RowSerializable> input = null;
    private Integer MODE;


    /**
     * Function to set program's variables
     * @throws Exception excepcion
     */
    @Before
    public void setUp() throws Exception {
        KUDU_MASTER = System.getProperty("kuduMaster", "localhost");
        TABLE_NAME = System.getProperty("tableName", "Table_1");
        TABLE_NAME2 = System.getProperty("tableName", "Table_2");
        MODE = KuduOutputFormat.CREATE;
        columnNames= new String[2];
        columnNames[0] = "key";
        columnNames[1] = "value";
        utils=new Utils("localhost");
        client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();


    }

    /**
     * Function to delete tables once finished the test
     * @throws Exception excepcion
     */
    @After
    public void tearDown() throws Exception {
        for(int i = 1; i<=10; i++)
            if(client.tableExists("Table_"+i))
                client.deleteTable("Table_"+i);
        client.shutdown();
        client.close();
    }

    /**
     * Main function which tests the program
     * @throws Exception excepcion
     *
     */
    @Test
    public void JobBatchSinkTest() throws Exception {

        //Test: Create a table
        assert !client.tableExists(TABLE_NAME) : "JUnit error: Table already exists";
        assert TestUtils.createTable(client,TABLE_NAME, "INT") : "JUnit test failed creating the table";
        assert !TestUtils.createTable(client,TABLE_NAME, "INT"): "JUnit test failed creating the table";
        assert TestUtils.scanRows(client, TABLE_NAME).size() == 0: "JUnit error: " + TABLE_NAME + " is not empty" ;
        //Test: Adding rows to the table
        assert TestUtils.insertData(client,TABLE_NAME, 0) : "JUnit test failed inserting data";
        assert TestUtils.scanRows(client,TABLE_NAME).isEmpty(): "JUnit error: " + TABLE_NAME + " is not empty" ;
        assert TestUtils.insertData(client,TABLE_NAME, 10) : "JUnit error inserting rows" ;
        assert !TestUtils.scanRows(client, TABLE_NAME).isEmpty(): "JUnit error: " + TABLE_NAME + " is empty" ;
        assert TestUtils.scanRows(client, TABLE_NAME).size() == 10: "JUnit error: " + TABLE_NAME + " must have 10 rows" ;
        /*If the data already exists in the db, it won't change*/
        assert TestUtils.insertData(client, TABLE_NAME, 5) : "JUnit test failed inserting data";
        assert TestUtils.scanRows(client, TABLE_NAME).size() == 10: "JUnit error: " + TABLE_NAME + " must have 10 rows";
        /*Test: Searching rows on the db*/
        assert TestUtils.scanRows(client, TABLE_NAME).contains("INT32 key=0, STRING value=This is the row number: 0"): "JUnit test failed scanning data from the table, expected row not read";
        assert TestUtils.scanRows(client, TABLE_NAME).contains("INT32 key=3, STRING value=This is the row number: 3"): "JUnit test failed scanning data from the table, expected row not read";
        assert !TestUtils.scanRows(client, TABLE_NAME).contains("INT32 key=6, STRING value=This is the row number: 3"): "JUnit test failed scanning data from the table, expected row not read";

        /*Executed with mode: CREATE*/
        assert setup2(KuduOutputFormat.CREATE): "JUnit failed while setting up the mode";
        env.execute();
        assert client.tableExists(TABLE_NAME2):"JUnit error: Table does not exists";
        assert TestUtils.scanRows(client, TABLE_NAME2).contains("INT32 key=0, STRING value=THIS IS THE ROW NUMBER: 0"): "JUnit failed: not expected record";
        assert TestUtils.scanRows(client, TABLE_NAME2).contains("INT32 key=6, STRING value=THIS IS THE ROW NUMBER: 3"): "JUnit failed: not expected record";
        assert TestUtils.numRows(client, TABLE_NAME2) == 10: "JUnit error: " + TABLE_NAME2 + " must have 10 rows";

        client.deleteTable(TABLE_NAME2);
        assert !client.tableExists(TABLE_NAME2);
        assert TestUtils.createTable(client, TABLE_NAME2, "INT");



        /*Changed mode to: APPEND*/
        assert setup2(KuduOutputFormat.APPEND);
        env.execute();
        assert client.tableExists(TABLE_NAME2);
        assert TestUtils.scanRows(client, TABLE_NAME2).contains("INT32 key=0, STRING value=THIS IS THE ROW NUMBER: 0"): "Junit failed: not expected record";
        assert TestUtils.scanRows(client, TABLE_NAME2).contains("INT32 key=18, STRING value=THIS IS THE ROW NUMBER: 9"): "Junit failed: not expected record";
        assert !TestUtils.scanRows(client, TABLE_NAME2).contains("INT32 key=2, STRING value=THIS IS THE ROW NUMBER: 2"): "Junit failed: not expected record";
        assert TestUtils.numRows(client,TABLE_NAME2) == 10;


        /*Changed mode to: OVERRIDE
        * This mode has the same tests than append mode
        */
        assert setup2(KuduOutputFormat.OVERRIDE);
        assert client.tableExists(TABLE_NAME2):"JUnit error: " + TABLE_NAME2 + " does not exists";
        env.execute();
        assert client.tableExists(TABLE_NAME2):"JUnit error: " + TABLE_NAME2 + " does not exists";
        assert TestUtils.scanRows(client, TABLE_NAME2).contains("INT32 key=0, STRING value=THIS IS THE ROW NUMBER: 0"): "Junit failed: not expected record";
        assert TestUtils.scanRows(client, TABLE_NAME2).contains("INT32 key=18, STRING value=THIS IS THE ROW NUMBER: 9"): "Junit failed: not expected record";
        assert !TestUtils.scanRows(client, TABLE_NAME2).contains("INT32 key=2, STRING value=THIS IS THE ROW NUMBER: 2"): "Junit failed: not expected record";
        assert TestUtils.numRows(client, TABLE_NAME2) == 10;

    }

    /**
     * Second setup used to change mode between CREATE, APPEND and OVERRIDE
     * @param mode New mode
     * @return False if there is any problem, True if not
     */
    private boolean setup2(Integer mode) {


        if (singleton) {
            KuduInputFormat KuduInputTest = new KuduInputFormat(TABLE_NAME, KUDU_MASTER);
            env = ExecutionEnvironment.getExecutionEnvironment();
            TypeInformation<RowSerializable> typeInformation = TypeInformation.of(RowSerializable.class);
            DataSet<RowSerializable> source = env.createInput(KuduInputTest, typeInformation);
            input = source.map(new TestUtils.MyMapFunction());
            singleton = false;
        }
        try {
            input.output(new KuduOutputFormat(KUDU_MASTER, TABLE_NAME2, columnNames, mode));
            return true;
        } catch (KuduException | KuduTableException | KuduClientException e) {
            return false;
        }


    }



}
