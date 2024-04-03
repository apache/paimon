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

package org.apache.paimon.mergetree;

import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.KeyValue;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactManager;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.memory.MemoryOwner;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.RecordWriter;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** A {@link RecordWriter} to write records and generate {@link CompactIncrement}. */
public class MergeTreeWriter implements RecordWriter<KeyValue>, MemoryOwner {

    private final boolean writeBufferSpillable;
    private final MemorySize maxDiskSize;
    private final int sortMaxFan;
    private final String sortCompression;
    private final IOManager ioManager;

    private final RowType keyType;
    private final RowType valueType;
    private final CompactManager compactManager;
    private final Comparator<InternalRow> keyComparator;
    private final MergeFunction<KeyValue> mergeFunction;
    private final KeyValueFileWriterFactory writerFactory;
    private final boolean commitForceCompact;
    private final ChangelogProducer changelogProducer;
    @Nullable private final FieldsComparator userDefinedSeqComparator;

    private final LinkedHashSet<DataFileMeta> newFiles;
    private final LinkedHashSet<DataFileMeta> deletedFiles;
    private final LinkedHashSet<DataFileMeta> newFilesChangelog;
    private final LinkedHashMap<String, DataFileMeta> compactBefore;
    private final LinkedHashSet<DataFileMeta> compactAfter;
    private final LinkedHashSet<DataFileMeta> compactChangelog;

    private long newSequenceNumber;
    private WriteBuffer writeBuffer;

    public MergeTreeWriter(
            boolean writeBufferSpillable,
            MemorySize maxDiskSize,
            int sortMaxFan,
            String sortCompression,
            IOManager ioManager,
            CompactManager compactManager,
            long maxSequenceNumber,
            Comparator<InternalRow> keyComparator,
            MergeFunction<KeyValue> mergeFunction,
            KeyValueFileWriterFactory writerFactory,
            boolean commitForceCompact,
            ChangelogProducer changelogProducer,
            @Nullable CommitIncrement increment,
            @Nullable FieldsComparator userDefinedSeqComparator) {
        this.writeBufferSpillable = writeBufferSpillable;
        this.maxDiskSize = maxDiskSize;
        this.sortMaxFan = sortMaxFan;
        this.sortCompression = sortCompression;
        this.ioManager = ioManager;
        this.keyType = writerFactory.keyType();
        this.valueType = writerFactory.valueType();
        this.compactManager = compactManager;
        this.newSequenceNumber = maxSequenceNumber + 1;
        this.keyComparator = keyComparator;
        this.mergeFunction = mergeFunction;
        this.writerFactory = writerFactory;
        this.commitForceCompact = commitForceCompact;
        this.changelogProducer = changelogProducer;
        this.userDefinedSeqComparator = userDefinedSeqComparator;

        this.newFiles = new LinkedHashSet<>();
        this.deletedFiles = new LinkedHashSet<>();
        this.newFilesChangelog = new LinkedHashSet<>();
        this.compactBefore = new LinkedHashMap<>();
        this.compactAfter = new LinkedHashSet<>();
        this.compactChangelog = new LinkedHashSet<>();
        if (increment != null) {
            newFiles.addAll(increment.newFilesIncrement().newFiles());
            deletedFiles.addAll(increment.newFilesIncrement().deletedFiles());
            newFilesChangelog.addAll(increment.newFilesIncrement().changelogFiles());
            increment
                    .compactIncrement()
                    .compactBefore()
                    .forEach(f -> compactBefore.put(f.fileName(), f));
            compactAfter.addAll(increment.compactIncrement().compactAfter());
            compactChangelog.addAll(increment.compactIncrement().changelogFiles());
        }
    }

    private long newSequenceNumber() {
        return newSequenceNumber++;
    }

    @VisibleForTesting
    CompactManager compactManager() {
        return compactManager;
    }

    @Override
    public void setMemoryPool(MemorySegmentPool memoryPool) {
        this.writeBuffer =
                new SortBufferWriteBuffer(
                        keyType,
                        valueType,
                        userDefinedSeqComparator,
                        memoryPool,
                        writeBufferSpillable,
                        maxDiskSize,
                        sortMaxFan,
                        sortCompression,
                        ioManager);
    }

    @Override
    public void write(KeyValue kv) throws Exception {
        long sequenceNumber = newSequenceNumber();
        boolean success = writeBuffer.put(sequenceNumber, kv.valueKind(), kv.key(), kv.value());
        if (!success) {
            flushWriteBuffer(false, false);
            success = writeBuffer.put(sequenceNumber, kv.valueKind(), kv.key(), kv.value());
            if (!success) {
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
    public long memoryOccupancy() {
        return writeBuffer.memoryOccupancy();
    }

    @Override
    public void flushMemory() throws Exception {
        boolean success = writeBuffer.flushMemory();
        if (!success) {
            flushWriteBuffer(false, false);
        }
    }

    private void flushWriteBuffer(boolean waitForLatestCompaction, boolean forcedFullCompaction)
            throws Exception {
        if (writeBuffer.size() > 0) {
            if (compactManager.shouldWaitForLatestCompaction()) {
                waitForLatestCompaction = true;
            }

            final RollingFileWriter<KeyValue, DataFileMeta> changelogWriter =
                    changelogProducer == ChangelogProducer.INPUT
                            ? writerFactory.createRollingChangelogFileWriter(0)
                            : null;
            final RollingFileWriter<KeyValue, DataFileMeta> dataWriter =
                    writerFactory.createRollingMergeTreeFileWriter(0, false);

            try {
                writeBuffer.forEach(
                        keyComparator,
                        mergeFunction,
                        changelogWriter == null ? null : changelogWriter::write,
                        dataWriter::write);
            } finally {
                if (changelogWriter != null) {
                    changelogWriter.close();
                }
                dataWriter.close();
            }

            if (changelogWriter != null) {
                newFilesChangelog.addAll(changelogWriter.result());
            }

            for (DataFileMeta fileMeta : dataWriter.result()) {
                newFiles.add(fileMeta);
                compactManager.addNewFile(fileMeta);
            }

            writeBuffer.clear();
        }

        trySyncLatestCompaction(waitForLatestCompaction);
        compactManager.triggerCompaction(forcedFullCompaction);
    }

    @Override
    public CommitIncrement prepareCommit(boolean waitCompaction) throws Exception {
        flushWriteBuffer(waitCompaction, false);
        if (commitForceCompact) {
            waitCompaction = true;
        }
        // Decide again whether to wait here.
        // For example, in the case of repeated failures in writing, it is possible that Level 0
        // files were successfully committed, but failed to restart during the compaction phase,
        // which may result in an increasing number of Level 0 files. This wait can avoid this
        // situation.
        if (compactManager.shouldWaitForPreparingCheckpoint()) {
            waitCompaction = true;
        }
        trySyncLatestCompaction(waitCompaction);
        return drainIncrement();
    }

    @Override
    public boolean isCompacting() {
        return compactManager.isCompacting();
    }

    @Override
    public void sync() throws Exception {
        trySyncLatestCompaction(true);
    }

    private CommitIncrement drainIncrement() {
        DataIncrement dataIncrement =
                new DataIncrement(
                        new ArrayList<>(newFiles),
                        new ArrayList<>(deletedFiles),
                        new ArrayList<>(newFilesChangelog));
        CompactIncrement compactIncrement =
                new CompactIncrement(
                        new ArrayList<>(compactBefore.values()),
                        new ArrayList<>(compactAfter),
                        new ArrayList<>(compactChangelog));

        newFiles.clear();
        deletedFiles.clear();
        newFilesChangelog.clear();
        compactBefore.clear();
        compactAfter.clear();
        compactChangelog.clear();

        return new CommitIncrement(dataIncrement, compactIncrement);
    }

    private void updateCompactResult(CompactResult result) {
        Set<String> afterFiles =
                result.after().stream().map(DataFileMeta::fileName).collect(Collectors.toSet());
        for (DataFileMeta file : result.before()) {
            if (compactAfter.remove(file)) {
                // This is an intermediate file (not a new data file), which is no longer needed
                // after compaction and can be deleted directly, but upgrade file is required by
                // previous snapshot and following snapshot, so we should ensure:
                // 1. This file is not the output of upgraded.
                // 2. This file is not the input of upgraded.
                if (!compactBefore.containsKey(file.fileName())
                        && !afterFiles.contains(file.fileName())) {
                    writerFactory.deleteFile(file.fileName(), file.level());
                }
            } else {
                compactBefore.put(file.fileName(), file);
            }
        }
        compactAfter.addAll(result.after());
        compactChangelog.addAll(result.changelog());
    }

    private void trySyncLatestCompaction(boolean blocking) throws Exception {
        Optional<CompactResult> result = compactManager.getCompactionResult(blocking);
        result.ifPresent(this::updateCompactResult);
    }

    @Override
    public void close() throws Exception {
        // cancel compaction so that it does not block job cancelling
        compactManager.cancelCompaction();
        sync();
        compactManager.close();

        // delete temporary files
        List<DataFileMeta> delete = new ArrayList<>(newFiles);
        newFiles.clear();
        deletedFiles.clear();

        for (DataFileMeta file : newFilesChangelog) {
            writerFactory.deleteFile(file.fileName(), file.level());
        }
        newFilesChangelog.clear();

        for (DataFileMeta file : compactAfter) {
            // upgrade file is required by previous snapshot, so we should ensure that this file is
            // not the output of upgraded.
            if (!compactBefore.containsKey(file.fileName())) {
                delete.add(file);
            }
        }

        compactAfter.clear();

        for (DataFileMeta file : compactChangelog) {
            writerFactory.deleteFile(file.fileName(), file.level());
        }
        compactChangelog.clear();

        for (DataFileMeta file : delete) {
            writerFactory.deleteFile(file.fileName(), file.level());
        }
    }
}
