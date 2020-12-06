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
package org.apache.flink.streaming.connectors.influxdb.sink.commiter;

import java.nio.ByteBuffer;
import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * This class Serialize and deserializes the commit values. Since we are sending the timestamp value
 * as a committable the Long object is (de)serialized.
 */
@Internal
public final class InfluxDBCommittableSerializer implements SimpleVersionedSerializer<Long> {

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(final Long timestamp) {
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(0, timestamp);
        return buffer.array();
    }

    @Override
    public Long deserialize(final int version, final byte[] serialized) {
        final ByteBuffer buffer = ByteBuffer.wrap(serialized);
        return buffer.getLong();
    }
}
