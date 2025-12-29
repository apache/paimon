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

package org.apache.paimon.globalindex;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.index.IndexPathFactory;

import java.io.IOException;
import java.util.UUID;

/** Helper class for managing global index files. */
public class GlobalIndexFileReadWrite implements GlobalIndexFileReader, GlobalIndexFileWriter {

    private final FileIO fileIO;
    private final IndexPathFactory indexPathFactory;

    public GlobalIndexFileReadWrite(FileIO fileIO, IndexPathFactory indexPathFactory) {
        this.fileIO = fileIO;
        this.indexPathFactory = indexPathFactory;
    }

    public String newFileName(String prefix) {
        return prefix + "-" + "global-index-" + UUID.randomUUID() + ".index";
    }

    @Override
    public Path filePath(String fileName) {
        return indexPathFactory.toPath(fileName);
    }

    public long fileSize(String fileName) throws IOException {
        return fileIO.getFileSize(filePath(fileName));
    }

    public PositionOutputStream newOutputStream(String fileName) throws IOException {
        return fileIO.newOutputStream(indexPathFactory.toPath(fileName), true);
    }

    public SeekableInputStream getInputStream(String fileName) throws IOException {
        Path path = indexPathFactory.toPath(fileName);
        return fileIO.newInputStream(path);
    }
}
