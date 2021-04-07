/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.pinot.filesystem;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * Defines the interaction with a shared filesystem. The shared filesystem must be accessible from all
 * nodes within the cluster than run a partition of the {@link org.apache.flink.streaming.connectors.pinot.PinotSink}.
 */
public interface FileSystemAdapter extends Serializable {

    /**
     * Writes a list of serialized elements to the shared filesystem.
     *
     * @param elements List of serialized elements
     * @return Path identifying the remote file
     * @throws IOException
     */
    String writeToSharedFileSystem(List<String> elements) throws IOException;

    /**
     * Reads a previously written list of serialized elements from the shared filesystem.
     *
     * @param path Path returned by {@link #writeToSharedFileSystem}
     * @return List of serialized elements read from the shared filesystem
     * @throws IOException
     */
    List<String> readFromSharedFileSystem(String path) throws IOException;

    /**
     * Deletes a file from the shared filesystem
     *
     * @param path Path returned by {@link #writeToSharedFileSystem}
     * @throws IOException
     */
    void deleteFromSharedFileSystem(String path) throws IOException;
}
