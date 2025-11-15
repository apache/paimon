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

package org.apache.paimon.index.globalindex;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;

public class GlobalIndexFileHelper {

    private final FileIO fileIO;
    private final IndexPathFactory indexPathFactory;

    public GlobalIndexFileHelper(FileIO fileIO, IndexPathFactory indexPathFactory) {
        this.fileIO = fileIO;
        this.indexPathFactory = indexPathFactory;
    }

    public String newFileName(String prefix) {
        return prefix + "-" + "global-index-" + UUID.randomUUID() + ".index";
    }

    public Path filePath(String fileName) {
        return indexPathFactory.newPath(fileName);
    }

    public OutputStream newOutputStream(String fileName) throws IOException {
        return fileIO.newOutputStream(indexPathFactory.newPath(fileName), true);
    }

    public SeekableInputStream getInputStream(IndexFileMeta indexFileMeta) throws IOException {
        Path path = indexPathFactory.toPath(indexFileMeta);
        return fileIO.newInputStream(path);
    }
}
