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
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.data.DataFilePathFactory;
import org.apache.flink.table.store.file.format.FileFormat;
import org.apache.flink.table.store.file.mergetree.Increment;
import org.apache.flink.table.store.file.stats.BinaryTableStats;
import org.apache.flink.table.store.file.stats.FieldStatsCollector;
import org.apache.flink.table.store.file.stats.FileStatsExtractor;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * A {@link RecordWriter} implementation that only accepts records which are always insert
 * operations and don't have any unique keys or sort keys.
 */
public class AppendOnlyWriter implements RecordWriter {
    private final BulkWriter.Factory<RowData> writerFactory;
    private final RowType writeSchema;
    private final long targetFileSize;
    private final DataFilePathFactory pathFactory;
    private final FileStatsExtractor fileStatsExtractor;
    private long nextSeqNum;

    private RowRollingWriter writer;

    public AppendOnlyWriter(
            FileFormat fileFormat,
            long targetFileSize,
            RowType writeSchema,
            long maxWroteSeqNumber,
            DataFilePathFactory pathFactory) {

        this.writerFactory = fileFormat.createWriterFactory(writeSchema);
        this.writeSchema = writeSchema;
        this.targetFileSize = targetFileSize;
        this.pathFactory = pathFactory;
        this.fileStatsExtractor = fileFormat.createStatsExtractor(writeSchema).orElse(null);
        this.nextSeqNum = maxWroteSeqNumber + 1;

        this.writer = createRollingRowWriter();
    }

    @Override
    public void write(ValueKind valueKind, RowData key, RowData value) throws Exception {
        Preconditions.checkArgument(
                valueKind == ValueKind.ADD,
                "Append-only writer cannot accept ValueKind: %s",
                valueKind);

        writer.write(value);
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
        sync();

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
                () -> new RowFileWriter(writerFactory, pathFactory.newPath()), targetFileSize);
    }

    private class RowRollingWriter extends RollingFileWriter<RowData, DataFileMeta> {

        public RowRollingWriter(Supplier<RowFileWriter> writerFactory, long targetFileSize) {
            super(writerFactory, targetFileSize);
        }
    }

    private class RowFileWriter extends BaseFileWriter<RowData, DataFileMeta> {
        private final long minSeqNum;
        private final FieldStatsCollector fieldStatsCollector;

        public RowFileWriter(BulkWriter.Factory<RowData> writerFactory, Path path) {
            super(writerFactory, path);
            this.minSeqNum = nextSeqNum;
            this.fieldStatsCollector = new FieldStatsCollector(writeSchema);
        }

        @Override
        public void write(RowData row) throws IOException {
            super.write(row);

            nextSeqNum += 1;
            if (fileStatsExtractor == null) {
                fieldStatsCollector.collect(row);
            }
        }

        @Override
        protected DataFileMeta createFileMeta(Path path) throws IOException {
            BinaryTableStats stats;
            if (fileStatsExtractor != null) {
                stats = fieldStatsCollector.toBinary(fileStatsExtractor.extract(path));
            } else {
                stats = fieldStatsCollector.extract();
            }

            return DataFileMeta.forAppend(
                    path.getName(),
                    FileUtils.getFileSize(path),
                    recordCount(),
                    stats,
                    minSeqNum,
                    Math.max(minSeqNum, nextSeqNum - 1));
        }
    }
}
