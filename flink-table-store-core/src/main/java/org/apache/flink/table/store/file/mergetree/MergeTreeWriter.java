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

package org.apache.flink.table.store.file.mergetree;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.util.MemorySegmentPool;
import org.apache.flink.table.store.CoreOptions.ChangelogProducer;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.compact.CompactManager;
import org.apache.flink.table.store.file.compact.CompactResult;
import org.apache.flink.table.store.file.io.DataFileMeta;
import org.apache.flink.table.store.file.io.KeyValueDataFileWriter;
import org.apache.flink.table.store.file.io.KeyValueFileWriterFactory;
import org.apache.flink.table.store.file.io.SingleFileWriter;
import org.apache.flink.table.store.file.memory.MemoryOwner;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunction;
import org.apache.flink.table.store.file.utils.RecordWriter;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** A {@link RecordWriter} to write records and generate {@link Increment}. */
public class MergeTreeWriter implements RecordWriter<KeyValue>, MemoryOwner {

    private final RowType keyType;

    private final RowType valueType;

    private final CompactManager compactManager;

    private final Levels levels;

    private final Comparator<RowData> keyComparator;

    private final MergeFunction mergeFunction;

    private final KeyValueFileWriterFactory writerFactory;

    private final boolean commitForceCompact;

    private final int numSortedRunStopTrigger;

    private final ChangelogProducer changelogProducer;

    private final LinkedHashSet<DataFileMeta> newFiles;

    private final LinkedHashMap<String, DataFileMeta> compactBefore;

    private final LinkedHashSet<DataFileMeta> compactAfter;

    private long newSequenceNumber;

    private MemTable memTable;

    public MergeTreeWriter(
            CompactManager compactManager,
            Levels levels,
            long maxSequenceNumber,
            Comparator<RowData> keyComparator,
            MergeFunction mergeFunction,
            KeyValueFileWriterFactory writerFactory,
            boolean commitForceCompact,
            int numSortedRunStopTrigger,
            ChangelogProducer changelogProducer) {
        this.keyType = writerFactory.keyType();
        this.valueType = writerFactory.valueType();
        this.compactManager = compactManager;
        this.levels = levels;
        this.newSequenceNumber = maxSequenceNumber + 1;
        this.keyComparator = keyComparator;
        this.mergeFunction = mergeFunction;
        this.writerFactory = writerFactory;
        this.commitForceCompact = commitForceCompact;
        this.numSortedRunStopTrigger = numSortedRunStopTrigger;
        this.changelogProducer = changelogProducer;
        this.newFiles = new LinkedHashSet<>();
        this.compactBefore = new LinkedHashMap<>();
        this.compactAfter = new LinkedHashSet<>();
    }

    private long newSequenceNumber() {
        return newSequenceNumber++;
    }

    @VisibleForTesting
    Levels levels() {
        return levels;
    }

    @Override
    public void setMemoryPool(MemorySegmentPool memoryPool) {
        this.memTable = new SortBufferMemTable(keyType, valueType, memoryPool);
    }

    @Override
    public void write(KeyValue kv) throws Exception {
        long sequenceNumber =
                kv.sequenceNumber() == KeyValue.UNKNOWN_SEQUENCE
                        ? newSequenceNumber()
                        : kv.sequenceNumber();
        boolean success = memTable.put(sequenceNumber, kv.valueKind(), kv.key(), kv.value());
        if (!success) {
            flushMemory();
            success = memTable.put(sequenceNumber, kv.valueKind(), kv.key(), kv.value());
            if (!success) {
                throw new RuntimeException("Mem table is too small to hold a single element.");
            }
        }
    }

    @Override
    public long memoryOccupancy() {
        return memTable.memoryOccupancy();
    }

    @Override
    public void flushMemory() throws Exception {
        if (memTable.size() > 0) {
            if (levels.numberOfSortedRuns() > numSortedRunStopTrigger) {
                // stop writing, wait for compaction finished
                finishCompaction(true);
            }

            // write changelog file
            List<String> extraFiles = new ArrayList<>();
            if (changelogProducer == ChangelogProducer.INPUT) {
                SingleFileWriter<KeyValue, Void> writer = writerFactory.createExtraFileWriter();
                writer.write(memTable.rawIterator());
                writer.close();
                extraFiles.add(writer.path().getName());
            }

            // write lsm level 0 file
            try {
                Iterator<KeyValue> iterator = memTable.mergeIterator(keyComparator, mergeFunction);
                KeyValueDataFileWriter writer = writerFactory.createLevel0Writer();
                writer.write(iterator);
                writer.close();

                DataFileMeta fileMeta = writer.result().copy(extraFiles);
                newFiles.add(fileMeta);
                levels.addLevel0File(fileMeta);
            } catch (Throwable e) {
                // exception occurs, clean up changelog file if needed
                for (String extraFile : extraFiles) {
                    writerFactory.deleteFile(extraFile);
                }
                throw e;
            }

            memTable.clear();
            submitCompaction();
        }
    }

    @Override
    public Increment prepareCommit(boolean endOfInput) throws Exception {
        flushMemory();
        boolean blocking = endOfInput || commitForceCompact;
        finishCompaction(blocking);
        return drainIncrement();
    }

    @Override
    public void sync() throws Exception {
        finishCompaction(true);
    }

    private Increment drainIncrement() {
        Increment increment =
                new Increment(
                        new ArrayList<>(newFiles),
                        new ArrayList<>(compactBefore.values()),
                        new ArrayList<>(compactAfter));
        newFiles.clear();
        compactBefore.clear();
        compactAfter.clear();
        return increment;
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
                    writerFactory.deleteFile(file.fileName());
                }
            } else {
                compactBefore.put(file.fileName(), file);
            }
        }
        compactAfter.addAll(result.after());
    }

    private void submitCompaction() throws Exception {
        finishCompaction(false);
        if (compactManager.isCompactionFinished()) {
            compactManager.submitCompaction();
        }
    }

    private void finishCompaction(boolean blocking) throws Exception {
        Optional<CompactResult> result = compactManager.finishCompaction(blocking);
        result.ifPresent(this::updateCompactResult);
    }

    @Override
    public List<DataFileMeta> close() throws Exception {
        // cancel compaction so that it does not block job cancelling
        compactManager.cancelCompaction();
        sync();

        // delete temporary files
        List<DataFileMeta> delete = new ArrayList<>(newFiles);
        for (DataFileMeta file : compactAfter) {
            // upgrade file is required by previous snapshot, so we should ensure that this file is
            // not the output of upgraded.
            if (!compactBefore.containsKey(file.fileName())) {
                delete.add(file);
            }
        }
        for (DataFileMeta file : delete) {
            writerFactory.deleteFile(file.fileName());
        }
        newFiles.clear();
        compactAfter.clear();
        return delete;
    }
}
