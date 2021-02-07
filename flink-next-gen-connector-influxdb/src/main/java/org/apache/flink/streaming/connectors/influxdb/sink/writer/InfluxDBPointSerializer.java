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
package org.apache.flink.streaming.connectors.influxdb.sink.writer;

import com.influxdb.client.write.Point;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import lombok.SneakyThrows;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.influxdb.common.InfluxParser;

public class InfluxDBPointSerializer implements SimpleVersionedSerializer<Point> {

    private static final Charset CHARSET = StandardCharsets.UTF_8;
    InfluxParser parser = new InfluxParser();

    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(final Point point) throws IOException {
        final byte[] serialized = point.toLineProtocol().getBytes(CHARSET);
        final byte[] targetBytes = new byte[Integer.BYTES + serialized.length];

        final ByteBuffer bb = ByteBuffer.wrap(targetBytes).order(ByteOrder.LITTLE_ENDIAN);
        bb.putInt(serialized.length);
        bb.put(serialized);
        return targetBytes;
    }

    @SneakyThrows
    @Override
    public Point deserialize(final int version, final byte[] serialized) throws IOException {
        final ByteBuffer bb = ByteBuffer.wrap(serialized).order(ByteOrder.LITTLE_ENDIAN);
        final byte[] targetStringBytes = new byte[bb.getInt()];
        bb.get(targetStringBytes);
        final String line = new String(targetStringBytes, CHARSET);
        return this.parser.parseToDataPoint(line).toPoint();
    }
}
