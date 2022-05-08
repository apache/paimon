/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.table.store.file.writer;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * The abstracted base file writer implementation for {@link FileWriter}.
 *
 * @param <T> record data type.
 * @param <R> file meta data type.
 */
public abstract class BaseFileWriter<T, R> implements FileWriter<T, R> {

    private final BulkWriter.Factory<T> writerFactory;
    private final Path path;

    private long recordCount;
    private FSDataOutputStream currentOut = null;
    private BulkWriter<T> currentWriter = null;

    private boolean closed = false;

    public BaseFileWriter(BulkWriter.Factory<T> writerFactory, Path path) {
        this.writerFactory = writerFactory;
        this.path = path;

        this.recordCount = 0;
    }

    public Path path() {
        return path;
    }

    private void openCurrentWriter() throws IOException {
        this.currentOut = path.getFileSystem().create(path, FileSystem.WriteMode.NO_OVERWRITE);
        this.currentWriter = writerFactory.create(currentOut);
    }

    @Override
    public void write(T row) throws IOException {
        if (currentWriter == null) {
            openCurrentWriter();
        }

        currentWriter.addElement(row);
        recordCount += 1;
    }

    @Override
    public long recordCount() {
        return recordCount;
    }

    @Override
    public long length() throws IOException {
        if (currentOut != null) {
            return currentOut.getPos();
        }
        return 0;
    }

    protected abstract R createFileMeta(Path path) throws IOException;

    @Override
    public void abort() {
        IOUtils.closeQuietly(this);

        // Abort to clean the orphan file.
        FileUtils.deleteOrWarn(path);
    }

    @Override
    public R result() throws IOException {
        Preconditions.checkState(closed, "Cannot access the file meta unless close this writer.");

        return createFileMeta(path);
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            if (currentWriter != null) {
                currentWriter.finish();
                currentWriter = null;
            }

            if (currentOut != null) {
                currentOut.close();
                currentOut = null;
            }

            closed = true;
        }
    }
}
