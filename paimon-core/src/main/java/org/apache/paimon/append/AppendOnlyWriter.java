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

package org.apache.paimon.append;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactManager;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.SpillableBuffer;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.NewFilesIncrement;
import org.apache.paimon.io.RowDataRollingFileWriter;
import org.apache.paimon.memory.MemoryOwner;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.statistics.FieldStatsCollector;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.RecordWriter;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * A {@link RecordWriter} implementation that only accepts records which are always insert
 * operations and don't have any unique keys or sort keys.
 */
public class AppendOnlyWriter implements RecordWriter<InternalRow>, MemoryOwner {

    private final FileIO fileIO;
    private final long schemaId;
    private final FileFormat fileFormat;
    private final long targetFileSize;
    private final RowType writeSchema;
    private final DataFilePathFactory pathFactory;
    private final CompactManager compactManager;
    private final boolean forceCompact;
    private final List<DataFileMeta> newFiles;
    private final List<DataFileMeta> compactBefore;
    private final List<DataFileMeta> compactAfter;
    private final LongCounter seqNumCounter;
    private final String fileCompression;
    private final boolean spillable;
    private final FieldStatsCollector.Factory[] statsCollectors;
    private final IOManager ioManager;

    private SpillableBuffer writeBuffer;

    public AppendOnlyWriter(
            FileIO fileIO,
            IOManager ioManager,
            long schemaId,
            FileFormat fileFormat,
            long targetFileSize,
            RowType writeSchema,
            long maxSequenceNumber,
            CompactManager compactManager,
            boolean forceCompact,
            DataFilePathFactory pathFactory,
            @Nullable CommitIncrement increment,
            boolean spillable,
            String fileCompression,
            FieldStatsCollector.Factory[] statsCollectors) {
        this.fileIO = fileIO;
        this.schemaId = schemaId;
        this.fileFormat = fileFormat;
        this.targetFileSize = targetFileSize;
        this.writeSchema = writeSchema;
        this.pathFactory = pathFactory;
        this.compactManager = compactManager;
        this.forceCompact = forceCompact;
        this.newFiles = new ArrayList<>();
        this.compactBefore = new ArrayList<>();
        this.compactAfter = new ArrayList<>();
        this.seqNumCounter = new LongCounter(maxSequenceNumber + 1);
        this.fileCompression = fileCompression;
        this.spillable = spillable;
        this.ioManager = ioManager;
        this.statsCollectors = statsCollectors;

        if (increment != null) {
            newFiles.addAll(increment.newFilesIncrement().newFiles());
            compactBefore.addAll(increment.compactIncrement().compactBefore());
            compactAfter.addAll(increment.compactIncrement().compactAfter());
        }
    }

    @Override
    public void write(InternalRow rowData) throws Exception {
        Preconditions.checkArgument(
                rowData.getRowKind() == RowKind.INSERT,
                "Append-only writer can only accept insert row kind, but current row kind is: %s",
                rowData.getRowKind());
        boolean success = writeBuffer.add(rowData);
        if (!success) {
            flushWriteBuffer(false, false);
            success = writeBuffer.add(rowData);
            if (!success) {
                // Should not get here, because writeBuffer will throw too big exception out.
                // But we throw again in case of something unexpected happens. (like someone changed
                // code in SpillableBuffer.)
                throw new RuntimeException("Mem table is too small to hold a single element.");
            }
        }
    }

    @Override
    public void compact(boolean fullCompaction) throws Exception {
        flushWriteBuffer(true, fullCompaction);
    }

    @Override
    public void addNewFiles(List<DataFileMeta> files) {
        files.forEach(compactManager::addNewFile);
    }

    @Override
    public Collection<DataFileMeta> dataFiles() {
        return compactManager.allFiles();
    }

    @Override
    public CommitIncrement prepareCommit(boolean waitCompaction) throws Exception {
        flushWriteBuffer(false, false);
        trySyncLatestCompaction(waitCompaction || forceCompact);
        return drainIncrement();
    }

    // This method flush writer buffer record to data file
    private void flushWriteBuffer(boolean waitForLatestCompaction, boolean forcedFullCompaction)
            throws Exception {
        List<DataFileMeta> flushedFiles = new ArrayList<>();
        if (writeBuffer != null) {
            writeBuffer.complete();
            RowDataRollingFileWriter writer = createRollingRowWriter();
            try (SpillableBuffer.BufferIterator iterator = writeBuffer.newIterator()) {
                while (iterator.advanceNext()) {
                    writer.write(iterator.getRow());
                }
            } finally {
                writer.close();
            }
            flushedFiles.addAll(writer.result());
            // reuse writeBuffer
            writeBuffer.reset();
        }

        // add new generated files
        flushedFiles.forEach(compactManager::addNewFile);
        trySyncLatestCompaction(waitForLatestCompaction);
        compactManager.triggerCompaction(forcedFullCompaction);
        newFiles.addAll(flushedFiles);
    }

    @Override
    public void sync() throws Exception {
        trySyncLatestCompaction(true);
    }

    @Override
    public void close() throws Exception {
        // cancel compaction so that it does not block job cancelling
        compactManager.cancelCompaction();
        sync();

        compactManager.close();
        for (DataFileMeta file : compactAfter) {
            // appendOnlyCompactManager will rewrite the file and no file upgrade will occur, so we
            // can directly delete the file in compactAfter.
            fileIO.deleteQuietly(pathFactory.toPath(file.fileName()));
        }

        if (writeBuffer != null) {
            writeBuffer.reset();
            // enable gc
            writeBuffer = null;
        }
    }

    private RowDataRollingFileWriter createRollingRowWriter() {
        return new RowDataRollingFileWriter(
                fileIO,
                schemaId,
                fileFormat,
                targetFileSize,
                writeSchema,
                pathFactory,
                seqNumCounter,
                fileCompression,
                statsCollectors);
    }

    private void trySyncLatestCompaction(boolean blocking)
            throws ExecutionException, InterruptedException {
        compactManager
                .getCompactionResult(blocking)
                .ifPresent(
                        result -> {
                            compactBefore.addAll(result.before());
                            compactAfter.addAll(result.after());
                        });
    }

    private CommitIncrement drainIncrement() {
        NewFilesIncrement newFilesIncrement =
                new NewFilesIncrement(new ArrayList<>(newFiles), Collections.emptyList());
        CompactIncrement compactIncrement =
                new CompactIncrement(
                        new ArrayList<>(compactBefore),
                        new ArrayList<>(compactAfter),
                        Collections.emptyList());

        newFiles.clear();
        compactBefore.clear();
        compactAfter.clear();

        return new CommitIncrement(newFilesIncrement, compactIncrement);
    }

    @Override
    public void setMemoryPool(MemorySegmentPool memoryPool) {
        this.writeBuffer =
                new SpillableBuffer(
                        ioManager, memoryPool, new InternalRowSerializer(writeSchema), spillable);
    }

    @Override
    public long memoryOccupancy() {
        return writeBuffer.memoryOccupancy();
    }

    @Override
    public void flushMemory() throws Exception {
        flushWriteBuffer(false, false);
    }

    @VisibleForTesting
    SpillableBuffer getWriteBuffer() {
        return writeBuffer;
    }

    @VisibleForTesting
    List<DataFileMeta> getNewFiles() {
        return newFiles;
    }
}
