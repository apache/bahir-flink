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

package org.apache.flink.streaming.connectors.pinot.v2.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Serializer for {@link PinotWriterState}
 */
@Internal
public class PinotWriterStateSerializer implements SimpleVersionedSerializer<PinotWriterState> {

    private static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(PinotWriterState writerState) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            out.writeLong(writerState.getMinTimestamp());
            out.writeLong(writerState.getMaxTimestamp());

            out.writeInt(writerState.getSerializedElements().size());
            for (String serialized : writerState.getSerializedElements()) {
                out.writeUTF(serialized);
            }

            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public PinotWriterState deserialize(int version, byte[] serialized) throws IllegalStateException, IOException {
        switch (version) {
            case 1:
                return deserializeV1(serialized);
            default:
                throw new IllegalStateException("Unrecognized version or corrupt state: " + version);
        }
    }

    private PinotWriterState deserializeV1(byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             DataInputStream in = new DataInputStream(bais)) {
            long minTimestamp = in.readLong();
            long maxTimestamp = in.readLong();

            long size = in.readInt();
            List<String> serializedElements = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                serializedElements.add(in.readUTF());
            }
            return new PinotWriterState(serializedElements, minTimestamp, maxTimestamp);
        }
    }
}
