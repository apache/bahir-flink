package org.apache.flink.streaming.connectors.redis;

import java.util.Map;
import org.apache.flink.table.descriptors.ConnectorDescriptor;

public class Redis extends ConnectorDescriptor {

    public Redis(String type, int version, boolean formatNeeded) {
        super(type, version, formatNeeded);
    }

    @Override
    protected Map<String, String> toConnectorProperties() {
        return null;
    }
}
