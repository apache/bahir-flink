package org.apache.flink.streaming.connectors.kudu.connect;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KuduTableContext {

    protected static final Logger LOG = LoggerFactory.getLogger(KuduTableContext.class);

    private static final Map<String, KuduTable> asyncCache = new HashMap<>();


    private KuduTableContext() { }

    public static final KuduTable getKuduTable(AsyncKuduClient client, KuduTableInfo infoTable) throws IOException {
        synchronized (asyncCache) {
            String tableName = infoTable.getName();
            if (!asyncCache.containsKey(tableName)) {
                asyncCache.put(tableName, table(client, infoTable));
                LOG.info("table cached {}", tableName);
            }
            return asyncCache.get(tableName);
        }
    }

    public static final boolean deleteKuduTable(AsyncKuduClient client, KuduTableInfo infoTable) throws IOException {
        synchronized (asyncCache) {
            String tableName = infoTable.getName();
            client.syncClient().deleteTable(tableName);
            if (asyncCache.containsKey(tableName)) {
                asyncCache.remove(tableName);
                LOG.info("table removed {}", tableName);
            }
            return !asyncCache.containsKey(tableName);
        }
    }


    private static KuduTable table(AsyncKuduClient asyncClient, KuduTableInfo infoTable) throws IOException {
        KuduClient client = asyncClient.syncClient();
        KuduTable table;

        String tableName = infoTable.getName();
        if (client.tableExists(tableName)) {
            table = client.openTable(tableName);
        } else if (infoTable.createIfNotExist()) {
            table = client.createTable(tableName, infoTable.getSchema(), infoTable.getCreateTableOptions());
        } else {
            throw new UnsupportedOperationException("table not exists and is marketed to not be created");
        }
        return table;
    }


}
