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

import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Writer to roll over to a new file if the current size exceed the target file size.
 *
 * @param <T> record data type.
 */
public class RollingFileWriter<T> implements FileWriter<T, List<DataFileMeta>> {

    private final Supplier<BaseFileWriter<T>> writerFactory;
    private final long targetFileSize;
    private final List<BaseFileWriter<T>> openedWriters;
    private final List<DataFileMeta> results;

    private BaseFileWriter<T> currentWriter = null;
    private long recordCount = 0;
    private boolean closed = false;

    public RollingFileWriter(Supplier<BaseFileWriter<T>> writerFactory, long targetFileSize) {
        this.writerFactory = writerFactory;
        this.targetFileSize = targetFileSize;
        this.openedWriters = new ArrayList<>();
        this.results = new ArrayList<>();
    }

    @Override
    public void write(T row) throws IOException {
        // Open the current writer if write the first record or roll over happen before.
        if (currentWriter == null) {
            openCurrentWriter();
        }

        currentWriter.write(row);
        recordCount += 1;

        if (currentWriter.length() >= targetFileSize) {
            closeCurrentWriter();
        }
    }

    private void openCurrentWriter() {
        currentWriter = writerFactory.get();
        openedWriters.add(currentWriter);
    }

    private void closeCurrentWriter() {
        if (currentWriter != null) {
            try {
                currentWriter.close();
                results.add(currentWriter.result());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            currentWriter = null;
        }
    }

    @Override
    public long recordCount() {
        return recordCount;
    }

    @Override
    public long length() throws IOException {
        long totalLength = results.stream().mapToLong(DataFileMeta::fileSize).sum();
        if (currentWriter != null) {
            totalLength += currentWriter.length();
        }

        return totalLength;
    }

    @Override
    public void flush() throws IOException {
        if (currentWriter != null) {
            currentWriter.flush();
        }
    }

    @Override
    public void abort() {
        closeCurrentWriter();
        openedWriters.stream().forEach(FileWriter::abort);
    }

    @Override
    public List<DataFileMeta> result() {
        Preconditions.checkState(closed, "Cannot access the results unless close all writers.");

        return results;
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closeCurrentWriter();

            closed = true;
        }
    }
}
