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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.mergetree.Increment;
import org.apache.flink.table.store.file.writer.RecordWriter;
import org.apache.flink.table.store.format.FileFormat;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

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
    private final List<DataFileMeta> toCompact;
    private final List<DataFileMeta> compactBefore;
    private final List<DataFileMeta> compactAfter;

    private AppendOnlyRollingFileWriter writer;

    public AppendOnlyWriter(
            long schemaId,
            FileFormat fileFormat,
            long targetFileSize,
            RowType writeSchema,
            List<DataFileMeta> restoredFiles,
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
        this.toCompact = new ArrayList<>(restoredFiles);
        this.compactBefore = new ArrayList<>();
        this.compactAfter = new ArrayList<>();
        this.writer = createRollingFileWriter(getMaxSequenceNumber(restoredFiles) + 1);
    }

    @Override
    public void write(RowData rowData) throws Exception {
        Preconditions.checkArgument(
                rowData.getRowKind() == RowKind.INSERT,
                "Append-only writer can only accept insert row kind, but current row kind is: %s",
                rowData.getRowKind());
        writer.write(rowData);
        if (!toCompact.isEmpty()) {
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
            writer = createRollingFileWriter(getMaxSequenceNumber(newFiles) + 1);
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

    private long getMaxSequenceNumber(List<DataFileMeta> fileMetas) {
        return fileMetas.stream()
                .map(DataFileMeta::maxSequenceNumber)
                .max(Long::compare)
                .orElse(-1L);
    }

    private AppendOnlyRollingFileWriter createRollingFileWriter(long nextSeqNum) {
        return new AppendOnlyRollingFileWriter(
                schemaId, fileFormat, targetFileSize, writeSchema, nextSeqNum, pathFactory);
    }

    private void submitCompaction() {
        if (compactManager.isCompactionFinished()) {
            compactManager.updateCompactBefore(toCompact);
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
                                toCompact.clear();
                                // if the last compacted file is still small, add it back
                                DataFileMeta lastFile =
                                        result.after().get(result.after().size() - 1);
                                if (lastFile.fileSize() < targetFileSize) {
                                    toCompact.add(lastFile);
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
        toCompact.addAll(newFiles);
        return increment;
    }

    @VisibleForTesting
    List<DataFileMeta> getToCompact() {
        return toCompact;
    }
}
