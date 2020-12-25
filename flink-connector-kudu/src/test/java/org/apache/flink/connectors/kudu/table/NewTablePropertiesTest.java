package org.apache.flink.connectors.kudu.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @fileName: NewTablePropertiesTest.java
 * @description: new tableProperties test
 * @author: by echo huang
 * @date: 2020/12/25 2:30 下午
 */
public class NewTablePropertiesTest extends KuduCatalogTest{
    private KuduCatalog catalog;
    private StreamTableEnvironment tableEnv;

    @BeforeEach
    public void init() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        catalog = new KuduCatalog(harness.getMasterAddressesAsString());
        tableEnv = KuduTableTestUtils.createTableEnvWithBlinkPlannerStreamingMode(env);
        tableEnv.registerCatalog("kudu", catalog);
        tableEnv.useCatalog("kudu");
    }

    @Test
    public void testHashPartitionNums() throws TableNotExistException {
        catalog.dropTable(ObjectPath.fromString(EnvironmentSettings.DEFAULT_BUILTIN_DATABASE + ".TestTable1"), true);
        catalog.dropTable(ObjectPath.fromString(EnvironmentSettings.DEFAULT_BUILTIN_DATABASE + ".TestTable2"), true);
        tableEnv.executeSql("CREATE TABLE TestTable1 (`first` STRING, `second` String) WITH ('kudu.hash-columns' = 'first', 'kudu.primary-key-columns' = 'first')");
        tableEnv.executeSql("CREATE TABLE TestTable2 (`first` STRING, `second` String) WITH ('kudu.hash-columns' = 'first','kudu.hash-partition-nums'='6', 'kudu.primary-key-columns' = 'first')");
    }


    @Test
    public void testOwner() throws TableNotExistException {
        catalog.dropTable(ObjectPath.fromString(EnvironmentSettings.DEFAULT_BUILTIN_DATABASE + ".TestTable1"), true);
        tableEnv.executeSql("CREATE TABLE TestTable1 (`first` STRING, `second` String) WITH ('kudu.hash-columns' = 'first', 'kudu.primary-key-columns' = 'first','kudu.table-owner'='admin')");
    }
}
