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

package org.apache.flink.table.store.file.data;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.format.FileFormat;
import org.apache.flink.table.store.file.mergetree.Increment;
import org.apache.flink.table.store.file.stats.BinaryTableStats;
import org.apache.flink.table.store.file.stats.FieldStatsArraySerializer;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.store.file.writer.BaseFileWriter;
import org.apache.flink.table.store.file.writer.FileWriter;
import org.apache.flink.table.store.file.writer.Metric;
import org.apache.flink.table.store.file.writer.MetricFileWriter;
import org.apache.flink.table.store.file.writer.RecordWriter;
import org.apache.flink.table.store.file.writer.RollingFileWriter;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A {@link RecordWriter} implementation that only accepts records which are always insert
 * operations and don't have any unique keys or sort keys.
 */
public class AppendOnlyWriter implements RecordWriter<RowData> {
    private final long schemaId;
    private final long targetFileSize;
    private final DataFilePathFactory pathFactory;
    private final FieldStatsArraySerializer statsArraySerializer;

    private final FileWriter.Factory<RowData, Metric> fileWriterFactory;
    private long nextSeqNum;

    private RowRollingWriter writer;

    public AppendOnlyWriter(
            long schemaId,
            FileFormat fileFormat,
            long targetFileSize,
            RowType writeSchema,
            long maxWroteSeqNumber,
            DataFilePathFactory pathFactory) {
        this.schemaId = schemaId;
        this.targetFileSize = targetFileSize;
        this.pathFactory = pathFactory;
        this.statsArraySerializer = new FieldStatsArraySerializer(writeSchema);

        // Initialize the file writer factory to write records and generic metric.
        this.fileWriterFactory =
                MetricFileWriter.createFactory(
                        fileFormat.createWriterFactory(writeSchema),
                        Function.identity(),
                        writeSchema,
                        fileFormat.createStatsExtractor(writeSchema).orElse(null));

        this.nextSeqNum = maxWroteSeqNumber + 1;
        this.writer = createRollingRowWriter();
    }

    @Override
    public void write(RowData rowData) throws Exception {
        Preconditions.checkArgument(
                rowData.getRowKind() == RowKind.INSERT,
                "Append-only writer can only accept insert row kind, but current row kind is: %s",
                rowData.getRowKind());
        writer.write(rowData);
    }

    @Override
    public Increment prepareCommit() throws Exception {
        List<DataFileMeta> newFiles = new ArrayList<>();

        if (writer != null) {
            writer.close();
            newFiles.addAll(writer.result());

            // Reopen the writer to accept further records.
            writer = createRollingRowWriter();
        }

        return Increment.forAppend(newFiles);
    }

    @Override
    public void sync() throws Exception {
        // Do nothing here, as this writer don't introduce any async compaction thread currently.
    }

    @Override
    public List<DataFileMeta> close() throws Exception {
        try {
            sync();
        } catch (Exception ignore) {
            // The thread pool is closed and the asynchronous task will be cancelled, an exception
            // may be thrown here, and we need to ignore it
        }

        List<DataFileMeta> result = new ArrayList<>();
        if (writer != null) {
            // Abort this writer to clear uncommitted files.
            writer.abort();

            result.addAll(writer.result());
            writer = null;
        }

        return result;
    }

    private RowRollingWriter createRollingRowWriter() {
        return new RowRollingWriter(
                () -> new RowFileWriter(fileWriterFactory, pathFactory.newPath()), targetFileSize);
    }

    private class RowRollingWriter extends RollingFileWriter<RowData, DataFileMeta> {

        public RowRollingWriter(Supplier<RowFileWriter> writerFactory, long targetFileSize) {
            super(writerFactory, targetFileSize);
        }
    }

    private class RowFileWriter extends BaseFileWriter<RowData, DataFileMeta> {
        private final long minSeqNum;

        public RowFileWriter(FileWriter.Factory<RowData, Metric> writerFactory, Path path) {
            super(writerFactory, path);
            this.minSeqNum = nextSeqNum;
        }

        @Override
        public void write(RowData row) throws IOException {
            super.write(row);

            nextSeqNum += 1;
        }

        @Override
        protected DataFileMeta createResult(Path path, Metric metric) throws IOException {
            BinaryTableStats stats = statsArraySerializer.toBinary(metric.fieldStats());

            return DataFileMeta.forAppend(
                    path.getName(),
                    FileUtils.getFileSize(path),
                    recordCount(),
                    stats,
                    minSeqNum,
                    Math.max(minSeqNum, nextSeqNum - 1),
                    schemaId);
        }
    }
}
