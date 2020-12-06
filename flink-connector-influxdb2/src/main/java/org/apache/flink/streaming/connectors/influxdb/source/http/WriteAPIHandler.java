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
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.streaming.connectors.influxdb.common.DataPoint;
import org.apache.flink.streaming.connectors.influxdb.common.InfluxParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class handles the incoming requests through the path /api/v2/write. The handle function
 * reads each line in the body and uses the {@link InfluxParser} to pars them to {@link DataPoint}
 * objects.
 */
@Internal
public final class WriteAPIHandler extends Handler {
    private static final Logger LOG = LoggerFactory.getLogger(WriteAPIHandler.class);

    private final int maximumLinesPerRequest;
    private final FutureCompletingBlockingQueue ingestionQueue;
    private final int threadIndex;
    private final long enqueueWaitTime;

    public WriteAPIHandler(
            final int maximumLinesPerRequest,
            final FutureCompletingBlockingQueue ingestionQueue,
            final int threadIndex,
            final long enqueueWaitTime) {
        this.maximumLinesPerRequest = maximumLinesPerRequest;
        this.ingestionQueue = ingestionQueue;
        this.threadIndex = threadIndex;
        this.enqueueWaitTime = enqueueWaitTime;
    }

    @Override
    public void handle(final HttpExchange t) throws IOException {
        final BufferedReader in =
                new BufferedReader(
                        new InputStreamReader(t.getRequestBody(), StandardCharsets.UTF_8));

        try {
            String line;
            final List<DataPoint> points = new ArrayList<>();
            int numberOfLinesParsed = 0;
            while ((line = in.readLine()) != null) {
                final DataPoint dataPoint = InfluxParser.parseToDataPoint(line);
                points.add(dataPoint);
                numberOfLinesParsed++;
                if (numberOfLinesParsed > this.maximumLinesPerRequest) {
                    throw new RequestTooLargeException(
                            String.format(
                                    "Payload too large. Maximum number of lines per request is %d.",
                                    this.maximumLinesPerRequest));
                }
            }

            final boolean result =
                    CompletableFuture.supplyAsync(
                                    () -> {
                                        try {
                                            return this.ingestionQueue.put(
                                                    this.threadIndex, points);
                                        } catch (final InterruptedException e) {
                                            return false;
                                        }
                                    })
                            .get(this.enqueueWaitTime, TimeUnit.SECONDS);

            if (!result) {
                throw new TimeoutException("Failed to enqueue");
            }

            t.sendResponseHeaders(HttpURLConnection.HTTP_NO_CONTENT, -1);
            this.ingestionQueue.notifyAvailable();
        } catch (final ParseException e) {
            Handler.sendResponse(t, HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());
        } catch (final RequestTooLargeException e) {
            Handler.sendResponse(t, HttpURLConnection.HTTP_ENTITY_TOO_LARGE, e.getMessage());
        } catch (final TimeoutException e) {
            Handler.sendResponse(t, HTTP_TOO_MANY_REQUESTS, "Server overloaded");
            LOG.error(e.getMessage());
        } catch (final ExecutionException | InterruptedException e) {
            Handler.sendResponse(t, HttpURLConnection.HTTP_INTERNAL_ERROR, "Server Error");
            LOG.error(e.getMessage());
        }
    }

    private static class RequestTooLargeException extends RuntimeException {
        RequestTooLargeException(final String message) {
            super(message);
        }
    }
}
