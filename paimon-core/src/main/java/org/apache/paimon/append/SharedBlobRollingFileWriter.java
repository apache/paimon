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

package org.apache.paimon.append;

import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.BlobRef;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.io.FileWriterAbortExecutor;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.io.SingleFileWriter;
import org.apache.paimon.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Rolls shared blob files only between descriptor groups.
 *
 * <p>The target size is soft: after it is reached, all immediately following rows with the same
 * exact descriptor remain in the current file. The next different descriptor, NULL, or placeholder
 * starts a new file.
 */
class SharedBlobRollingFileWriter<R> implements RollingFileWriter<InternalRow, R> {

    private static final Logger LOG = LoggerFactory.getLogger(SharedBlobRollingFileWriter.class);

    private final Supplier<? extends SingleFileWriter<InternalRow, R>> writerFactory;
    private final long targetFileSize;
    private final List<FileWriterAbortExecutor> closedWriters = new ArrayList<>();
    private final List<R> results = new ArrayList<>();

    private @Nullable SingleFileWriter<InternalRow, R> currentWriter;
    private @Nullable BlobDescriptor currentGroup;
    private long recordCount;
    private boolean pendingRoll;
    private boolean closed;

    SharedBlobRollingFileWriter(
            Supplier<? extends SingleFileWriter<InternalRow, R>> writerFactory,
            long targetFileSize) {
        this.writerFactory = writerFactory;
        this.targetFileSize = targetFileSize;
    }

    @Override
    public void write(InternalRow row) throws IOException {
        try {
            BlobDescriptor nextGroup = descriptor(row);
            if (currentWriter != null && pendingRoll && !Objects.equals(currentGroup, nextGroup)) {
                closeCurrentWriter();
            }
            if (currentWriter == null) {
                currentWriter = writerFactory.get();
            }

            currentWriter.write(row);
            recordCount++;
            currentGroup = nextGroup;
            if (currentWriter.reachTargetSize(
                    recordCount % CHECK_ROLLING_RECORD_CNT == 0, targetFileSize)) {
                pendingRoll = true;
            }
        } catch (Throwable e) {
            LOG.warn(
                    "Exception occurs when writing shared blob file {}. Cleaning up.",
                    currentWriter == null ? null : currentWriter.path(),
                    e);
            abort();
            throw e;
        }
    }

    @Override
    public void writeBundle(BundleRecords records) throws IOException {
        for (InternalRow row : records) {
            write(row);
        }
    }

    @Override
    public long recordCount() {
        return recordCount;
    }

    @Override
    public void abort() {
        if (currentWriter != null) {
            currentWriter.abort();
            currentWriter = null;
        }
        for (FileWriterAbortExecutor abortExecutor : closedWriters) {
            abortExecutor.abort();
        }
    }

    @Override
    public List<R> result() {
        Preconditions.checkState(closed, "Cannot access the results unless close all writers.");
        return results;
    }

    List<FileWriterAbortExecutor> drainAbortExecutors() {
        Preconditions.checkState(closed, "Cannot drain abort executors unless close all writers.");
        List<FileWriterAbortExecutor> result = new ArrayList<>(closedWriters);
        closedWriters.clear();
        return result;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        try {
            closeCurrentWriter();
        } catch (IOException e) {
            abort();
            throw e;
        } finally {
            closed = true;
        }
    }

    private void closeCurrentWriter() throws IOException {
        if (currentWriter == null) {
            return;
        }
        currentWriter.close();
        currentWriter.abortExecutor().ifPresent(closedWriters::add);
        results.add(currentWriter.result());
        currentWriter = null;
        currentGroup = null;
        pendingRoll = false;
    }

    private static @Nullable BlobDescriptor descriptor(InternalRow row) {
        if (row.isNullAt(0)) {
            return null;
        }
        Blob blob = row.getBlob(0);
        return blob != null && blob.getClass() == BlobRef.class ? blob.toDescriptor() : null;
    }
}
