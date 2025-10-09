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

package org.apache.paimon.io;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.types.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Format table's writer to roll over to a new file if the current size exceed the target file size.
 */
public class FormatTableRollingFileWriter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(FormatTableRollingFileWriter.class);

    private static final int CHECK_ROLLING_RECORD_CNT = 1000;

    private final Supplier<FormatTableSingleFileWriter> writerFactory;
    private final long targetFileSize;
    private final List<FileWriterAbortExecutor> closedWriters;
    private final List<TwoPhaseOutputStream.Committer> committers;

    private FormatTableSingleFileWriter currentWriter = null;
    private long recordCount = 0;
    private boolean closed = false;

    public FormatTableRollingFileWriter(
            FileIO fileIO,
            FileFormat fileFormat,
            long targetFileSize,
            RowType writeSchema,
            DataFilePathFactory pathFactory,
            String fileCompression) {
        this.writerFactory =
                () ->
                        new FormatTableSingleFileWriter(
                                fileIO,
                                fileFormat.createWriterFactory(writeSchema),
                                pathFactory.newPath(),
                                fileCompression);
        this.targetFileSize = targetFileSize;
        this.closedWriters = new ArrayList<>();
        this.committers = new ArrayList<>();
    }

    public long targetFileSize() {
        return targetFileSize;
    }

    public void write(InternalRow row) throws IOException {
        try {
            if (currentWriter == null) {
                currentWriter = writerFactory.get();
            }

            currentWriter.write(row);
            recordCount += 1;
            boolean needRolling =
                    currentWriter.reachTargetSize(
                            recordCount % CHECK_ROLLING_RECORD_CNT == 0, targetFileSize);
            if (needRolling) {
                closeCurrentWriter();
            }
        } catch (Throwable e) {
            LOG.warn(
                    "Exception occurs when writing file {}. Cleaning up.",
                    currentWriter == null ? null : currentWriter.path(),
                    e);
            abort();
            throw e;
        }
    }

    private void closeCurrentWriter() throws IOException {
        if (currentWriter == null) {
            return;
        }

        currentWriter.close();
        closedWriters.add(currentWriter.abortExecutor());
        if (currentWriter.committers() != null) {
            committers.addAll(currentWriter.committers());
        }

        currentWriter = null;
    }

    public void abort() {
        if (currentWriter != null) {
            currentWriter.abort();
        }
        for (FileWriterAbortExecutor abortExecutor : closedWriters) {
            abortExecutor.abort();
        }
    }

    public List<TwoPhaseOutputStream.Committer> committers() {
        return committers;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        try {
            closeCurrentWriter();
        } catch (IOException e) {
            LOG.warn(
                    "Exception occurs when writing file {}. Cleaning up.", currentWriter.path(), e);
            abort();
            throw e;
        } finally {
            closed = true;
        }
    }
}
