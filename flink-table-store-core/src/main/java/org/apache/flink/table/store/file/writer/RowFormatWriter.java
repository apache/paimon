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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.stats.FieldStats;
import org.apache.flink.table.store.file.stats.FieldStatsCollector;
import org.apache.flink.table.store.file.stats.FileStatsExtractor;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * A {@link FormatWriter} implementation that write {@link RowData} with generated {@link Metric}.
 */
public class RowFormatWriter implements FormatWriter<RowData> {
    private static final Logger LOG = LoggerFactory.getLogger(RowFormatWriter.class);

    private final BulkWriter<RowData> writer;
    private final FSDataOutputStream out;
    private final Path path;
    private final FileStatsExtractor fileStatsExtractor;

    private FieldStatsCollector fieldStatsCollector = null;

    private long recordCount;
    private boolean closed = false;

    private RowFormatWriter(
            BulkWriter<RowData> writer,
            FSDataOutputStream out,
            Path path,
            RowType writeSchema,
            FileStatsExtractor fileStatsExtractor) {
        this.writer = writer;
        this.out = out;
        this.path = path;

        this.fileStatsExtractor = fileStatsExtractor;
        if (this.fileStatsExtractor == null) {
            this.fieldStatsCollector = new FieldStatsCollector(writeSchema);
        }

        this.recordCount = 0L;
    }

    @Override
    public void write(RowData record) throws IOException {
        writer.addElement(record);

        if (fieldStatsCollector != null) {
            fieldStatsCollector.collect(record);
        }

        recordCount += 1;
    }

    @Override
    public long recordCount() {
        return recordCount;
    }

    @Override
    public long length() {
        try {
            return out.getPos();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void abort() {
        try {
            close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        FileUtils.deleteOrWarn(path);
    }

    @Override
    public Metric result() throws IOException {
        Preconditions.checkState(closed, "Cannot access metric unless the writer is closed.");

        FieldStats[] stats;
        if (fileStatsExtractor != null) {
            stats = fileStatsExtractor.extract(path);
        } else {
            stats = fieldStatsCollector.extractFieldStats();
        }

        return new Metric(stats, recordCount);
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            if (writer != null) {
                writer.flush();
                writer.finish();
            }

            if (out != null) {
                out.flush();
                out.close();
            }

            closed = true;
        }
    }

    /** A factory that creates {@link RowFormatWriter}. */
    public static class RowFormatWriterFactory implements FormatWriter.Factory<RowData> {

        private final BulkWriter.Factory<RowData> writerFactory;
        private final RowType writeSchema;
        private final FileStatsExtractor fileStatsExtractor;

        public RowFormatWriterFactory(
                BulkWriter.Factory<RowData> writerFactory,
                RowType writeSchema,
                FileStatsExtractor fileStatsExtractor) {
            this.writerFactory = writerFactory;
            this.writeSchema = writeSchema;
            this.fileStatsExtractor = fileStatsExtractor;
        }

        @Override
        public FormatWriter<RowData> create(Path path) throws IOException {

            FileSystem fs = path.getFileSystem();
            FSDataOutputStream out = fs.create(path, FileSystem.WriteMode.NO_OVERWRITE);

            try {
                BulkWriter<RowData> bulkWriter = writerFactory.create(out);
                return new RowFormatWriter(bulkWriter, out, path, writeSchema, fileStatsExtractor);
            } catch (Throwable e) {
                LOG.error("Failed to create the Row bulk writer for path: {}", path, e);

                out.close();
                throw e;
            }
        }
    }
}
