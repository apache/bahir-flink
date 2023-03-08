/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.influxdb.source.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.apache.flink.annotation.Internal;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;

/** Abstract base handle class for creating a response */
@Internal
abstract class Handler implements HttpHandler {

    static final int HTTP_TOO_MANY_REQUESTS = 415;

    static void sendResponse(
            @NotNull final HttpExchange t, final int responseCode, @NotNull final String message)
            throws IOException {
        final byte[] response = message.getBytes();
        t.sendResponseHeaders(responseCode, response.length);
        final OutputStream os = t.getResponseBody();
        os.write(response);
        os.close();
    }
}
