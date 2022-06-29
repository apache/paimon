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
import org.apache.flink.table.store.file.mergetree.Increment;
import org.apache.flink.table.store.file.stats.BinaryTableStats;
import org.apache.flink.table.store.file.stats.FieldStatsArraySerializer;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.store.file.writer.BaseFileWriter;
import org.apache.flink.table.store.file.writer.FileWriter;
import org.apache.flink.table.store.file.writer.Metric;
import org.apache.flink.table.store.file.writer.RecordWriter;
import org.apache.flink.table.store.file.writer.RollingFileWriter;
import org.apache.flink.table.store.format.FileFormat;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

/**
 * A {@link RecordWriter} implementation that only accepts records which are always insert
 * operations and don't have any unique keys or sort keys.
 */
public class AppendOnlyWriter implements RecordWriter<RowData> {

    private final long schemaId;
    private final long targetFileSize;
    private final DataFilePathFactory pathFactory;
    private final FileWriter.Factory<RowData, Metric> fileWriterFactory;
    private final FieldStatsArraySerializer statsArraySerializer;
    private final AppendOnlyCompactManager compactManager;
    private final boolean forceCompact;
    private final List<DataFileMeta> toCompact;
    private final List<DataFileMeta> compactBefore;
    private final List<DataFileMeta> compactAfter;

    private long nextSeqNum;
    private RowRollingWriter writer;

    public AppendOnlyWriter(
            long schemaId,
            long targetFileSize,
            FileWriter.Factory<RowData, Metric> fileWriterFactory,
            FieldStatsArraySerializer statsArraySerializer,
            AppendOnlyCompactManager compactManager,
            boolean forceCompact,
            List<DataFileMeta> restoredFiles,
            long maxWroteSeqNumber,
            DataFilePathFactory pathFactory) {
        this.schemaId = schemaId;
        this.targetFileSize = targetFileSize;
        this.pathFactory = pathFactory;
        this.fileWriterFactory = fileWriterFactory;
        this.statsArraySerializer = statsArraySerializer;
        this.compactManager = compactManager;
        this.forceCompact = forceCompact;
        this.toCompact = new ArrayList<>(restoredFiles);
        this.compactBefore = new ArrayList<>();
        this.compactAfter = new ArrayList<>();

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
        if (compactManager.isCompactionFinished() && !toCompact.isEmpty()) {
            submitCompaction();
        }
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
        finishCompaction(forceCompact);
        return drainIncrement(newFiles);
    }

    @Override
    public void sync() throws Exception {
        finishCompaction(true);
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

    private void submitCompaction() {
        if (compactManager.isCompactionFinished()) {
            compactManager.submitCompaction(toCompact);
        }
    }

    private void finishCompaction(boolean blocking)
            throws ExecutionException, InterruptedException {
        compactManager
                .finishCompaction(blocking)
                .ifPresent(
                        result -> {
                            compactBefore.addAll(result.before());
                            compactAfter.addAll(result.after());
                        });
        if (blocking && !compactAfter.isEmpty()) {
            toCompact.clear();
        }
    }

    private Increment drainIncrement(List<DataFileMeta> newFiles) {
        Increment increment =
                new Increment(
                        newFiles, new ArrayList<>(compactBefore), new ArrayList<>(compactAfter));
        compactBefore.clear();
        compactAfter.clear();
        toCompact.addAll(newFiles);
        return increment;
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

    /**
     * A {@link BaseFileWriter} implementation to write a single file with a fixed range of sequence
     * number.
     */
    public static class SingleFileWriter extends BaseFileWriter<RowData, DataFileMeta> {
        private final FieldStatsArraySerializer statsArraySerializer;
        private final long schemaId;
        private final long minSeqNum;
        private final long maxSeqNum;

        public SingleFileWriter(
                FileWriter.Factory<RowData, Metric> writerFactory,
                Path path,
                FieldStatsArraySerializer statsArraySerializer,
                long minSeqNum,
                long maxSeqNum,
                long schemaId) {
            super(writerFactory, path);
            this.statsArraySerializer = statsArraySerializer;
            this.schemaId = schemaId;
            this.minSeqNum = minSeqNum;
            this.maxSeqNum = maxSeqNum;
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
                    maxSeqNum,
                    schemaId);
        }
    }
}
