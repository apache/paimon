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
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.store.file.mergetree.compact.Accumulator;
import org.apache.flink.table.store.file.mergetree.compact.CompactManager;
import org.apache.flink.table.store.file.mergetree.sst.SstFile;
import org.apache.flink.table.store.file.mergetree.sst.SstFileMeta;
import org.apache.flink.table.store.file.utils.RecordWriter;
import org.apache.flink.util.CloseableIterator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

/** A {@link RecordWriter} to write records and generate {@link Increment}. */
public class MergeTreeWriter implements RecordWriter {

    private final MemTable memTable;

    private final CompactManager compactManager;

    private final Levels levels;

    private final Comparator<RowData> keyComparator;

    private final Accumulator accumulator;

    private final SstFile sstFile;

    private final boolean commitForceCompact;

    private final LinkedHashSet<SstFileMeta> newFiles;

    private final LinkedHashSet<SstFileMeta> compactBefore;

    private final LinkedHashSet<SstFileMeta> compactAfter;

    private long newSequenceNumber;

    public MergeTreeWriter(
            MemTable memTable,
            CompactManager compactManager,
            Levels levels,
            long maxSequenceNumber,
            Comparator<RowData> keyComparator,
            Accumulator accumulator,
            SstFile sstFile,
            boolean commitForceCompact) {
        this.memTable = memTable;
        this.compactManager = compactManager;
        this.levels = levels;
        this.newSequenceNumber = maxSequenceNumber + 1;
        this.keyComparator = keyComparator;
        this.accumulator = accumulator;
        this.sstFile = sstFile;
        this.commitForceCompact = commitForceCompact;
        this.newFiles = new LinkedHashSet<>();
        this.compactBefore = new LinkedHashSet<>();
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
    public void write(ValueKind valueKind, RowData key, RowData value) throws Exception {
        long sequenceNumber = newSequenceNumber();
        boolean success = memTable.put(sequenceNumber, valueKind, key, value);
        if (!success) {
            flush();
            success = memTable.put(sequenceNumber, valueKind, key, value);
            if (!success) {
                throw new RuntimeException("Mem table is too small to hold a single element.");
            }
        }
    }

    private void flush() throws Exception {
        if (memTable.size() > 0) {
            finishCompaction();
            Iterator<KeyValue> iterator = memTable.iterator(keyComparator, accumulator);
            List<SstFileMeta> files =
                    sstFile.write(CloseableIterator.adapterForIterator(iterator), 0);
            newFiles.addAll(files);
            files.forEach(levels::addLevel0File);
            memTable.clear();
            submitCompaction();
        }
    }

    @Override
    public Increment prepareCommit() throws Exception {
        flush();
        if (commitForceCompact) {
            finishCompaction();
        }
        return drainIncrement();
    }

    @Override
    public void sync() throws Exception {
        finishCompaction();
    }

    private Increment drainIncrement() {
        Increment increment =
                new Increment(
                        new ArrayList<>(newFiles),
                        new ArrayList<>(compactBefore),
                        new ArrayList<>(compactAfter));
        newFiles.clear();
        compactBefore.clear();
        compactAfter.clear();
        return increment;
    }

    private void updateCompactResult(CompactManager.CompactResult result) {
        for (SstFileMeta file : result.before()) {
            boolean removed = compactAfter.remove(file);
            if (removed) {
                // This is an intermediate file (not a new data file), which is no longer needed
                // after compaction and can be deleted directly
                sstFile.delete(file);
            } else {
                compactBefore.add(file);
            }
        }
        compactAfter.addAll(result.after());
    }

    private void submitCompaction() {
        compactManager.submitCompaction(levels);
    }

    private void finishCompaction() throws ExecutionException, InterruptedException {
        Optional<CompactManager.CompactResult> result = compactManager.finishCompaction(levels);
        if (result.isPresent()) {
            updateCompactResult(result.get());
        }
    }

    @Override
    public List<SstFileMeta> close() {
        // delete temporary files
        List<SstFileMeta> delete = new ArrayList<>(newFiles);
        delete.addAll(compactAfter);
        for (SstFileMeta file : delete) {
            sstFile.delete(file);
        }
        newFiles.clear();
        compactAfter.clear();
        return delete;
    }
}
