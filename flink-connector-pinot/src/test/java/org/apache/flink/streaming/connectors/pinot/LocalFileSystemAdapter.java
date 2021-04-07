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

package org.apache.flink.streaming.connectors.pinot;

import org.apache.flink.streaming.connectors.pinot.filesystem.FileSystemAdapter;
import org.apache.flink.streaming.connectors.pinot.filesystem.FileSystemUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The LocalFileSystemAdapter is used when sharing files via the local filesystem.
 * Keep in mind that using this FileSystemAdapter requires running the Flink app on a single node.
 */
public class LocalFileSystemAdapter implements FileSystemAdapter {

    private final String tempDirPrefix;

    public LocalFileSystemAdapter(String tempDirPrefix) {
        this.tempDirPrefix = checkNotNull(tempDirPrefix);
    }

    /**
     * Writes a list of serialized elements to the local filesystem.
     *
     * @param elements List of serialized elements
     * @return Path identifying the written file
     * @throws IOException
     */
    @Override
    public String writeToSharedFileSystem(List<String> elements) throws IOException {
        File tempDir = Files.createTempDirectory(tempDirPrefix).toFile();
        return FileSystemUtils.writeToLocalFile(elements, tempDir).getAbsolutePath();
    }

    /**
     * Reads a previously written list of serialized elements from the local filesystem.
     *
     * @param path Path returned by {@link #writeToSharedFileSystem}
     * @return List of serialized elements read from the local filesystem
     * @throws IOException
     */
    @Override
    public List<String> readFromSharedFileSystem(String path) throws IOException {
        File dataFile = new File(path);
        return Files.readAllLines(dataFile.toPath(), Charset.defaultCharset());
    }

    /**
     * Deletes a file from the local filesystem
     *
     * @param path Path returned by {@link #writeToSharedFileSystem}
     * @throws IOException
     */
    @Override
    public void deleteFromSharedFileSystem(String path) {
        new File(path).delete();
    }
}
