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
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.store.format.FieldStats;
import org.apache.flink.table.store.format.FieldStatsCollector;
import org.apache.flink.table.store.format.FileStatsExtractor;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Function;

/**
 * An {@link FileWriter} to write generic record and generate {@link Metric} once closing it.
 *
 * @param <T> generic record type.
 */
public class MetricFileWriter<T> implements FileWriter<T, Metric> {
    private static final Logger LOG = LoggerFactory.getLogger(MetricFileWriter.class);

    private final BulkWriter<RowData> writer;
    private final Function<T, RowData> converter;
    private final FSDataOutputStream out;
    private final Path path;
    @Nullable private final FileStatsExtractor fileStatsExtractor;

    private FieldStatsCollector fieldStatsCollector = null;

    private long recordCount;
    private long length;
    private boolean closed = false;

    private MetricFileWriter(
            BulkWriter<RowData> writer,
            Function<T, RowData> converter,
            FSDataOutputStream out,
            Path path,
            RowType writeSchema,
            @Nullable FileStatsExtractor fileStatsExtractor) {
        this.writer = writer;
        this.converter = converter;
        this.out = out;
        this.path = path;

        this.fileStatsExtractor = fileStatsExtractor;
        if (this.fileStatsExtractor == null) {
            this.fieldStatsCollector = new FieldStatsCollector(writeSchema);
        }

        this.recordCount = 0L;
        this.length = 0L;
    }

    @Override
    public void write(T record) throws IOException {
        RowData rowData = converter.apply(record);
        writer.addElement(rowData);

        if (fieldStatsCollector != null) {
            fieldStatsCollector.collect(rowData);
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
            stats = fieldStatsCollector.extract();
        }

        return new Metric(stats, recordCount, length);
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

                length = out.getPos();
                out.close();
            }

            closed = true;
        }
    }

    public static <T> FileWriter.Factory<T, Metric> createFactory(
            BulkWriter.Factory<RowData> factory,
            Function<T, RowData> converter,
            RowType writeSchema,
            @Nullable FileStatsExtractor fileStatsExtractor) {

        return path -> {
            // Open the output stream.
            FileSystem fs = path.getFileSystem();
            FSDataOutputStream out = fs.create(path, FileSystem.WriteMode.NO_OVERWRITE);

            try {
                return new MetricFileWriter<>(
                        factory.create(out), converter, out, path, writeSchema, fileStatsExtractor);
            } catch (Throwable e) {
                LOG.warn(
                        "Failed to open the bulk writer, closing the output stream and throw the error.",
                        e);

                IOUtils.closeQuietly(out);
                throw e;
            }
        };
    }
}
