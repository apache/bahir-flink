package org.apache.flink.streaming.connectors.activemq.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;


public class ActiveMQOptions {
    public static final ConfigOption<String> BROKER_URL =
            ConfigOptions.key("brokerUrl")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Required ActiveMQ broker url");
    public static final ConfigOption<String> DESTINATION_NAME =
            ConfigOptions.key("destinationName")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Required ActiveMQ destination name");

    public static final ConfigOption<String> DESTINATION_TYPE =
            ConfigOptions.key("destinationType")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Required ActiveMQ destination name");

    public static final ConfigOption<String> FORMAT =
            ConfigOptions.key("format")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Defines the format to serialize or deserialize data");

    public static final ConfigOption<Boolean> PERSISTENT_DELIVERY =
            ConfigOptions.key("persistentDelivery")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Determines whether the data is persistent");

}
