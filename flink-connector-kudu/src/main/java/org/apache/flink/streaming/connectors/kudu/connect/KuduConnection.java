package org.apache.flink.streaming.connectors.kudu.connect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.KuduClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KuduConnection implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(KuduConnection.class);

    private static final Map<String, AsyncKuduClient> asyncCache = new HashMap<>();

    private KuduConnection() { }

    public static final AsyncKuduClient getAsyncClient(String kuduMasters) {
        synchronized (asyncCache) {
            if (!asyncCache.containsKey(kuduMasters)) {
                asyncCache.put(kuduMasters, new AsyncKuduClient.AsyncKuduClientBuilder(kuduMasters).build());
                LOG.info("created new connection to {}", kuduMasters);
            }
            return asyncCache.get(kuduMasters);
        }
    }

    public static final KuduClient getSyncClient(String kuduMasters) {
        return getAsyncClient(kuduMasters).syncClient();
    }

    public static final void closeAsyncClient(String kuduMasters) {
        synchronized (asyncCache) {
            try {
                if (asyncCache.containsKey(kuduMasters)) {
                    asyncCache.remove(kuduMasters).close();
                    LOG.info("closed connection to {}", kuduMasters);
                }
            } catch (Exception e) {
                LOG.error("could not close connection to {}", kuduMasters);
            }
        }
    }

    @Override
    public void close() throws Exception {
        new ArrayList<>(asyncCache.keySet())
                .forEach(KuduConnection::closeAsyncClient);
    }


}
