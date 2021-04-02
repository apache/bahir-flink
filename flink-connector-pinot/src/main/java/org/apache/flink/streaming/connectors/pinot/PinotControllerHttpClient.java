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
import org.apache.http.StatusLine;
import org.apache.http.client.methods.*;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Helpers to interact with the Pinot controller via its public API.
 */
@Internal
public class PinotControllerHttpClient implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(PinotControllerHttpClient.class);
    private final String controllerHostPort;
    private final CloseableHttpClient httpClient;

    /**
     * @param controllerHost Pinot controller's host
     * @param controllerPort Pinot controller's port
     */
    public PinotControllerHttpClient(String controllerHost, String controllerPort) {
        checkNotNull(controllerHost);
        checkNotNull(controllerPort);
        this.controllerHostPort = String.format("http://%s:%s", controllerHost, controllerPort);
        this.httpClient = HttpClients.createDefault();
    }

    /**
     * Issues a request to the Pinot controller API.
     *
     * @param request Request to issue
     * @return Api response
     * @throws IOException
     */
    private ApiResponse execute(HttpRequestBase request) throws IOException {
        ApiResponse result;

        try (CloseableHttpResponse response = httpClient.execute(request)) {
            String body = EntityUtils.toString(response.getEntity());
            result = new ApiResponse(response.getStatusLine(), body);
        }

        return result;
    }

    /**
     * Issues a POST request to the Pinot controller API.
     *
     * @param path Path to POST to
     * @param body Request's body
     * @return API response
     * @throws IOException
     */
    ApiResponse post(String path, String body) throws IOException {
        HttpPost httppost = new HttpPost(this.controllerHostPort + path);
        httppost.setEntity(new StringEntity(body, ContentType.APPLICATION_JSON));
        LOG.debug("Posting string entity {} to {}", body, path);
        return this.execute(httppost);
    }

    /**
     * Issues a GET request to the Pinot controller API.
     *
     * @param path Path to GET from
     * @return API response
     * @throws IOException
     */
    ApiResponse get(String path) throws IOException {
        HttpGet httpget = new HttpGet(this.controllerHostPort + path);
        LOG.debug("Sending GET request to {}", path);
        return this.execute(httpget);
    }

    /**
     * Issues a DELETE request to the Pinot controller API.
     *
     * @param path Path to issue DELETE request to
     * @return API response
     * @throws IOException
     */
    ApiResponse delete(String path) throws IOException {
        HttpDelete httpdelete = new HttpDelete(this.controllerHostPort + path);
        LOG.debug("Sending DELETE request to {}", path);
        return this.execute(httpdelete);
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
    }

    /**
     * Helper class for wrapping Pinot controller API responses.
     */
    static class ApiResponse {
        public final StatusLine statusLine;
        public final String responseBody;

        ApiResponse(StatusLine statusLine, String responseBody) {
            this.statusLine = statusLine;
            this.responseBody = responseBody;
        }
    }
}
