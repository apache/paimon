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

package org.apache.paimon.index;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import java.io.IOException;
import java.io.UncheckedIOException;

/** Base index file. */
public abstract class IndexFile {

    protected final FileIO fileIO;

    protected final IndexPathFactory pathFactory;

    public IndexFile(FileIO fileIO, IndexPathFactory pathFactory) {
        this.fileIO = fileIO;
        this.pathFactory = pathFactory;
    }

    public Path path(IndexFileMeta file) {
        return pathFactory.toPath(file);
    }

    public long fileSize(IndexFileMeta file) {
        return fileSize(path(file));
    }

    public long fileSize(Path file) {
        try {
            return fileIO.getFileSize(file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void delete(IndexFileMeta file) {
        fileIO.deleteQuietly(path(file));
    }

    public boolean exists(IndexFileMeta file) {
        try {
            return fileIO.exists(path(file));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public boolean isExternalPath() {
        return pathFactory.isExternalPath();
    }
}
