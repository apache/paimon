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
    private final List<DataFileMeta> results;

    private BaseFileWriter<T> currentWriter = null;
    private long recordCount = 0;
    private boolean closed = false;

    public RollingFileWriter(Supplier<BaseFileWriter<T>> writerFactory, long targetFileSize) {
        this.writerFactory = writerFactory;
        this.targetFileSize = targetFileSize;
        this.results = new ArrayList<>();
    }

    @Override
    public void write(T row) throws IOException {
        // Open the current writer if write the first record or roll over happen before.
        if (currentWriter == null) {
            currentWriter = writerFactory.get();
        }

        currentWriter.write(row);
        recordCount += 1;

        if (currentWriter.length() >= targetFileSize) {
            currentWriter.close();
            results.add(currentWriter.result());

            currentWriter = null;
        }
    }

    @Override
    public long recordCount() {
        return recordCount;
    }

    @Override
    public long length() throws IOException {
        long lengthOfClosedFiles = results.stream().mapToLong(DataFileMeta::fileSize).sum();
        if (currentWriter != null) {
            lengthOfClosedFiles += currentWriter.length();
        }

        return lengthOfClosedFiles;
    }

    @Override
    public void flush() throws IOException {
        if (currentWriter != null) {
            currentWriter.flush();
        }
    }

    @Override
    public void abort() {
        // TODO abort to delete all created files.
    }

    @Override
    public List<DataFileMeta> result() {
        Preconditions.checkState(closed, "Cannot access the results unless close all writers.");

        return results;
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            if (currentWriter != null) {
                currentWriter.close();
                currentWriter = null;
            }

            closed = true;
        }
    }
}
