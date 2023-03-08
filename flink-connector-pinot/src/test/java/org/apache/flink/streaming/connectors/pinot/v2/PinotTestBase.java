/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.pinot.v2;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.streaming.connectors.pinot.PinotClusterContainer;
import org.apache.flink.streaming.connectors.pinot.PinotTestHelper;
import org.apache.flink.streaming.connectors.pinot.v2.external.JsonSerializer;
import org.apache.flink.util.TestLogger;
import org.apache.pinot.spi.config.table.*;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Base class for PinotSink e2e tests
 */
public class PinotTestBase extends TestLogger {

    protected static final Logger LOG = LoggerFactory.getLogger(PinotTestBase.class);

    private static final PinotClusterContainer pinotCluster = new PinotClusterContainer();
    protected static TableConfig TABLE_CONFIG;
    protected static final Schema TABLE_SCHEMA = PinotTableConfig.getTableSchema();
    protected static PinotTestHelper pinotHelper;

    @BeforeAll
    public static void setUpAll() {
        pinotCluster.start();
    }

    /**
     * Creates a new instance of the {@link PinotTestHelper} using the testcontainer port mappings
     * and creates the test table.
     *
     * @throws IOException
     */
    @BeforeEach
    public void setUp() throws IOException {
        TABLE_CONFIG = PinotTableConfig.getTableConfig();
        pinotHelper = new PinotTestHelper(getPinotHost(), getPinotControllerPort(), getPinotBrokerPort());
        pinotHelper.createTable(TABLE_CONFIG, TABLE_SCHEMA);
    }

    /**
     * Delete the test table after each test.
     *
     * @throws Exception
     */
    @AfterEach
    public void tearDown() throws Exception {
        pinotHelper.deleteTable(TABLE_CONFIG, TABLE_SCHEMA);
    }

    /**
     * Returns the current Pinot table name
     *
     * @return Pinot table name
     */
    protected String getTableName() {
        return TABLE_CONFIG.getTableName();
    }

    /**
     * Returns the host the Pinot container is available at
     *
     * @return Pinot container host
     */
    protected String getPinotHost() {
        return pinotCluster.getControllerHostAndPort().getHostText();
    }


    /**
     * Returns the Pinot controller port from the container ports.
     *
     * @return Pinot controller port
     */
    protected String getPinotControllerPort() {
        return String.valueOf(pinotCluster.getControllerHostAndPort().getPort());
    }

    /**
     * Returns the Pinot broker port from the container ports.
     *
     * @return Pinot broker port
     */
    private String getPinotBrokerPort() {
        return String.valueOf(pinotCluster.getBrokerHostAndPort().getPort());
    }

    /**
     * Class defining the elements passed to the {@link PinotSink} during the tests.
     */
    protected static class SingleColumnTableRow {

        private String _col1;
        private Long _timestamp;

        SingleColumnTableRow(@JsonProperty(value = "col1", required = true) String col1,
                             @JsonProperty(value = "timestamp", required = true) Long timestamp) {
            this._col1 = col1;
            this._timestamp = timestamp;
        }

        @JsonProperty("col1")
        public String getCol1() {
            return this._col1;
        }

        public void setCol1(String _col1) {
            this._col1 = _col1;
        }

        @JsonProperty("timestamp")
        public Long getTimestamp() {
            return this._timestamp;
        }

        public void setTimestamp(Long timestamp) {
            this._timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "SingleColumnTableRow{" +
                    "_col1='" + _col1 + '\'' +
                    ", _timestamp=" + _timestamp +
                    '}';
        }
    }

    /**
     * Serializes {@link SingleColumnTableRow} to JSON.
     */
    protected static class SingleColumnTableRowSerializer extends JsonSerializer<SingleColumnTableRow> {

        @Override
        public String toJson(SingleColumnTableRow element) {
            return JsonUtils.objectToJsonNode(element).toString();
        }
    }

    /**
     * Pinot table configuration helpers.
     */
    private static class PinotTableConfig {

        static final String TABLE_NAME_PREFIX = "FLTable";
        static final String SCHEMA_NAME = "FLTableSchema";

        private static SegmentsValidationAndRetentionConfig getValidationConfig() {
            SegmentsValidationAndRetentionConfig validationConfig = new SegmentsValidationAndRetentionConfig();
            validationConfig.setSegmentAssignmentStrategy("BalanceNumSegmentAssignmentStrategy");
            validationConfig.setSegmentPushType("APPEND");
            validationConfig.setSchemaName(SCHEMA_NAME);
            validationConfig.setReplication("1");
            return validationConfig;
        }

        private static TenantConfig getTenantConfig() {
            TenantConfig tenantConfig = new TenantConfig("DefaultTenant", "DefaultTenant", null);
            return tenantConfig;
        }

        private static IndexingConfig getIndexingConfig() {
            IndexingConfig indexingConfig = new IndexingConfig();
            return indexingConfig;
        }

        private static TableCustomConfig getCustomConfig() {
            TableCustomConfig customConfig = new TableCustomConfig(null);
            return customConfig;
        }

        private static String generateTableName() {
            // We want to use a new table name for each test in order to prevent interference
            // with segments that were pushed in the previous test,
            // but whose indexing by Pinot was delayed (thus, the previous test must have failed).
            return String.format("%s_%d", TABLE_NAME_PREFIX, System.currentTimeMillis());
        }

        static TableConfig getTableConfig() {
            return new TableConfig(
                    generateTableName(),
                    TableType.OFFLINE.name(),
                    getValidationConfig(),
                    getTenantConfig(),
                    getIndexingConfig(),
                    getCustomConfig(),
                    null, null, null, null, null,
                    null, null, null, null
            );
        }

        static Schema getTableSchema() {
            Schema schema = new Schema();
            schema.setSchemaName(SCHEMA_NAME);
            schema.addField(new DimensionFieldSpec("col1", FieldSpec.DataType.STRING, true));
            schema.addField(new DimensionFieldSpec("timestamp", FieldSpec.DataType.STRING, true));
            return schema;
        }
    }
}
