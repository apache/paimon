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

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * The abstracted base file writer implementation for {@link FileWriter}.
 *
 * @param <T> record data type.
 * @param <R> file meta data type.
 */
public abstract class BaseFileWriter<T, R> implements FileWriter<T, R> {

    private final FileWriter<T, Metric> writer;
    private final Path path;

    private Metric metric = null;
    private boolean closed = false;

    public BaseFileWriter(FileWriter.Factory<T, Metric> writerFactory, Path path) {
        try {
            this.writer = writerFactory.create(path);
        } catch (IOException e) {
            FileUtils.deleteOrWarn(path);
            throw new UncheckedIOException(e);
        }
        this.path = path;
    }

    public Path path() {
        return path;
    }

    @Override
    public void write(T row) throws IOException {
        writer.write(row);
    }

    @Override
    public long recordCount() {
        return writer.recordCount();
    }

    @Override
    public long length() throws IOException {
        return writer.length();
    }

    protected abstract R createResult(Path path, Metric metric) throws IOException;

    @Override
    public void abort() {
        IOUtils.closeQuietly(this);

        // Abort to clean the orphan file.
        FileUtils.deleteOrWarn(path);
    }

    @Override
    public R result() throws IOException {
        Preconditions.checkState(closed, "Cannot access the file meta unless close this writer.");
        Preconditions.checkNotNull(metric, "Metric cannot be null.");

        return createResult(path, metric);
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            writer.close();
            metric = writer.result();
            closed = true;
        }
    }
}
