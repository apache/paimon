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

package org.apache.paimon.fs;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.metrics.IOMetrics;

import java.io.IOException;

/**
 * FileIOWrapper is a wrapper class for the {@link FileIO}.
 *
 * <p>It allows users to monitor and track file I/O operations (e.g., read, write, delete, rename).
 */
public class MetricsFileIO implements FileIO {

    protected final FileIO fileIO;
    protected IOMetrics ioMetrics = null;

    public MetricsFileIO(FileIO fileIO) {
        this.fileIO = fileIO;
    }

    public MetricsFileIO withMetrics(IOMetrics ioMetrics) {
        this.ioMetrics = ioMetrics;
        return this;
    }

    public FileIO getFileIOInternal() {
        FileIO currentFileIO = fileIO;
        while (currentFileIO instanceof MetricsFileIO) {
            currentFileIO = ((MetricsFileIO) currentFileIO).fileIO;
        }
        return currentFileIO;
    }

    @VisibleForTesting
    public Boolean isMetricsEnabled() {
        return ioMetrics != null;
    }

    @Override
    public boolean isObjectStore() {
        return fileIO.isObjectStore();
    }

    @Override
    public void configure(CatalogContext context) {
        fileIO.configure(context);
    }

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        SeekableInputStream inputStream = fileIO.newInputStream(path);
        return new SeekableInputStreamIOWrapper(inputStream, this.ioMetrics);
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        PositionOutputStream outputStream = fileIO.newOutputStream(path, overwrite);
        return new PositionOutputStreamIOWrapper(outputStream, this.ioMetrics);
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        return fileIO.getFileStatus(path);
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        return fileIO.listStatus(path);
    }

    @Override
    public boolean exists(Path path) throws IOException {
        return fileIO.exists(path);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        return fileIO.delete(path, recursive);
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        return fileIO.mkdirs(path);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        return fileIO.rename(src, dst);
    }
}
