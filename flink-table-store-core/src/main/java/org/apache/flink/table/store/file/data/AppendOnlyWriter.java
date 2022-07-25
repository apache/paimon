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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
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
import org.apache.flink.table.store.format.FileFormat;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.flink.table.store.file.data.AppendOnlyWriter.RowRollingWriter.createRollingRowWriter;

/**
 * A {@link RecordWriter} implementation that only accepts records which are always insert
 * operations and don't have any unique keys or sort keys.
 */
public class AppendOnlyWriter implements RecordWriter<RowData> {

    private final long schemaId;
    private final FileFormat fileFormat;
    private final long targetFileSize;
    private final RowType writeSchema;
    private final DataFilePathFactory pathFactory;
    private final AppendOnlyCompactManager compactManager;
    private final boolean forceCompact;
    private final LinkedList<DataFileMeta> toCompact;
    private final List<DataFileMeta> compactBefore;
    private final List<DataFileMeta> compactAfter;
    private final LongCounter seqNumCounter;

    private RowRollingWriter writer;

    public AppendOnlyWriter(
            long schemaId,
            FileFormat fileFormat,
            long targetFileSize,
            RowType writeSchema,
            LinkedList<DataFileMeta> restoredFiles,
            AppendOnlyCompactManager compactManager,
            boolean forceCompact,
            DataFilePathFactory pathFactory) {
        this.schemaId = schemaId;
        this.fileFormat = fileFormat;
        this.targetFileSize = targetFileSize;
        this.writeSchema = writeSchema;
        this.pathFactory = pathFactory;
        this.compactManager = compactManager;
        this.forceCompact = forceCompact;
        this.toCompact = restoredFiles;
        this.compactBefore = new ArrayList<>();
        this.compactAfter = new ArrayList<>();
        this.seqNumCounter = new LongCounter(getMaxSequenceNumber(restoredFiles) + 1);
        this.writer =
                createRollingRowWriter(
                        schemaId,
                        fileFormat,
                        targetFileSize,
                        writeSchema,
                        pathFactory,
                        seqNumCounter);
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
    public Increment prepareCommit(boolean endOnfInput) throws Exception {
        List<DataFileMeta> newFiles = new ArrayList<>();
        if (writer != null) {
            writer.close();
            newFiles.addAll(writer.result());

            // Reopen the writer to accept further records.
            seqNumCounter.resetLocal();
            seqNumCounter.add(getMaxSequenceNumber(newFiles) + 1);
            writer =
                    createRollingRowWriter(
                            schemaId,
                            fileFormat,
                            targetFileSize,
                            writeSchema,
                            pathFactory,
                            seqNumCounter);
        }
        // add new generated files
        toCompact.addAll(newFiles);
        submitCompaction();

        boolean blocking = endOnfInput || forceCompact;
        finishCompaction(blocking);

        return drainIncrement(newFiles);
    }

    @Override
    public void sync() throws Exception {
        finishCompaction(true);
    }

    @Override
    public List<DataFileMeta> close() throws Exception {
        // cancel compaction so that it does not block job cancelling
        compactManager.cancelCompaction();
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

    private static long getMaxSequenceNumber(List<DataFileMeta> fileMetas) {
        return fileMetas.stream()
                .map(DataFileMeta::maxSequenceNumber)
                .max(Long::compare)
                .orElse(-1L);
    }

    private void submitCompaction() throws ExecutionException, InterruptedException {
        finishCompaction(false);
        if (compactManager.isCompactionFinished() && !toCompact.isEmpty()) {
            compactManager.submitCompaction();
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
                            if (!result.after().isEmpty()) {
                                // if the last compacted file is still small,
                                // add it back to the head
                                DataFileMeta lastFile =
                                        result.after().get(result.after().size() - 1);
                                if (lastFile.fileSize() < targetFileSize) {
                                    toCompact.offerFirst(lastFile);
                                }
                            }
                        });
    }

    private Increment drainIncrement(List<DataFileMeta> newFiles) {
        Increment increment =
                new Increment(
                        newFiles, new ArrayList<>(compactBefore), new ArrayList<>(compactAfter));
        compactBefore.clear();
        compactAfter.clear();
        return increment;
    }

    @VisibleForTesting
    List<DataFileMeta> getToCompact() {
        return toCompact;
    }

    /** Rolling file writer for append-only table. */
    public static class RowRollingWriter extends RollingFileWriter<RowData, DataFileMeta> {

        public RowRollingWriter(Supplier<RowFileWriter> writerFactory, long targetFileSize) {
            super(writerFactory, targetFileSize);
        }

        public static RowRollingWriter createRollingRowWriter(
                long schemaId,
                FileFormat fileFormat,
                long targetFileSize,
                RowType writeSchema,
                DataFilePathFactory pathFactory,
                LongCounter seqNumCounter) {
            return new RowRollingWriter(
                    () ->
                            new RowFileWriter(
                                    MetricFileWriter.createFactory(
                                            fileFormat.createWriterFactory(writeSchema),
                                            Function.identity(),
                                            writeSchema,
                                            fileFormat
                                                    .createStatsExtractor(writeSchema)
                                                    .orElse(null)),
                                    pathFactory.newPath(),
                                    writeSchema,
                                    schemaId,
                                    seqNumCounter),
                    targetFileSize);
        }

        public List<DataFileMeta> write(CloseableIterator<RowData> iterator) throws Exception {
            try {
                super.write(iterator);
                super.close();
                return super.result();
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                iterator.close();
            }
        }
    }

    /**
     * A {@link BaseFileWriter} impl with a counter with an initial value to record each row's
     * sequence number.
     */
    public static class RowFileWriter extends BaseFileWriter<RowData, DataFileMeta> {

        private final FieldStatsArraySerializer statsArraySerializer;
        private final long schemaId;
        private final LongCounter seqNumCounter;

        public RowFileWriter(
                FileWriter.Factory<RowData, Metric> writerFactory,
                Path path,
                RowType writeSchema,
                long schemaId,
                LongCounter seqNumCounter) {
            super(writerFactory, path);
            this.statsArraySerializer = new FieldStatsArraySerializer(writeSchema);
            this.schemaId = schemaId;
            this.seqNumCounter = seqNumCounter;
        }

        @Override
        public void write(RowData row) throws IOException {
            super.write(row);
            seqNumCounter.add(1L);
        }

        @Override
        protected DataFileMeta createResult(Path path, Metric metric) throws IOException {
            BinaryTableStats stats = statsArraySerializer.toBinary(metric.fieldStats());

            return DataFileMeta.forAppend(
                    path.getName(),
                    FileUtils.getFileSize(path),
                    recordCount(),
                    stats,
                    seqNumCounter.getLocalValue() - super.recordCount(),
                    seqNumCounter.getLocalValue() - 1,
                    schemaId);
        }
    }
}
