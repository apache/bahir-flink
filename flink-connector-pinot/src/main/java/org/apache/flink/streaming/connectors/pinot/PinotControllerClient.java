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

package org.apache.flink.streaming.connectors.pinot;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.connectors.pinot.exceptions.PinotControllerApiException;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * Helpers to interact with the Pinot controller via its public API.
 */
@Internal
public class PinotControllerClient implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(PinotControllerClient.class);
    private final PinotControllerHttpClient httpClient;

    /**
     * @param controllerHost Pinot controller's host
     * @param controllerPort Pinot controller's port
     */
    public PinotControllerClient(String controllerHost, String controllerPort) {
        this.httpClient = new PinotControllerHttpClient(controllerHost, controllerPort);
    }

    /**
     * Checks whether the provided segment name is registered with the given table.
     *
     * @param tableName   Target table's name
     * @param segmentName Segment name to check
     * @return True if segment with the provided name exists
     * @throws IOException
     */
    public boolean tableHasSegment(String tableName, String segmentName) throws IOException {
        PinotControllerHttpClient.ApiResponse res = httpClient.get(String.format("/tables/%s/%s/metadata", tableName, segmentName));

        if (res.statusLine.getStatusCode() == 200) {
            // A segment named `segmentName` exists within the table named `tableName`
            return true;
        }
        if (res.statusLine.getStatusCode() == 404) {
            // There is no such segment named `segmentName` within the table named `tableName`
            // (or the table named `tableName` does not exist)
            return false;
        }

        // Received an unexpected status code
        throw new PinotControllerApiException(res.responseBody);
    }

    /**
     * Deletes a segment from a table.
     *
     * @param tableName   Target table's name
     * @param segmentName Identifies the segment to delete
     * @throws IOException
     */
    public void deleteSegment(String tableName, String segmentName) throws IOException {
        PinotControllerHttpClient.ApiResponse res = httpClient.delete(String.format("/tables/%s/%s", tableName, segmentName));

        if (res.statusLine.getStatusCode() != 200) {
            LOG.error("Could not delete segment {} from table {}. Pinot controller returned: {}", tableName, segmentName, res.responseBody);
            throw new PinotControllerApiException(res.responseBody);
        }
    }

    /**
     * Fetches a Pinot table's schema via the Pinot controller API.
     *
     * @param tableName Target table's name
     * @return Pinot table schema
     * @throws IOException
     */
    public Schema getSchema(String tableName) throws IOException {
        Schema schema;
        PinotControllerHttpClient.ApiResponse res = httpClient.get(String.format("/tables/%s/schema", tableName));
        LOG.debug("Get schema request for table {} returned {}", tableName, res.responseBody);

        if (res.statusLine.getStatusCode() != 200) {
            throw new PinotControllerApiException(res.responseBody);
        }

        try {
            schema = JsonUtils.stringToObject(res.responseBody, Schema.class);
        } catch (IOException e) {
            throw new PinotControllerApiException("Caught exception while reading schema from Pinot Controller's response: " + res.responseBody);
        }
        LOG.debug("Retrieved schema: {}", schema.toSingleLineJsonString());
        return schema;
    }

    /**
     * Fetches a Pinot table's configuration via the Pinot controller API.
     *
     * @param tableName Target table's name
     * @return Pinot table configuration
     * @throws IOException
     */
    public TableConfig getTableConfig(String tableName) throws IOException {
        TableConfig tableConfig;
        PinotControllerHttpClient.ApiResponse res = httpClient.get(String.format("/tables/%s", tableName));
        LOG.debug("Get table config request for table {} returned {}", tableName, res.responseBody);

        try {
            String tableConfigAsJson = JsonUtils.stringToJsonNode(res.responseBody).get("OFFLINE").toString();
            tableConfig = JsonUtils.stringToObject(tableConfigAsJson, TableConfig.class);
        } catch (IOException e) {
            throw new PinotControllerApiException("Caught exception while reading table config from Pinot Controller's response: " + res.responseBody);
        }
        LOG.debug("Retrieved table config: {}", tableConfig.toJsonString());
        return tableConfig;
    }


    @Override
    public void close() throws IOException {
        httpClient.close();
    }
}
