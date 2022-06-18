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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.data.DataFilePathFactory;
import org.apache.flink.table.store.file.data.DataFileWriter;
import org.apache.flink.table.store.file.format.FileFormat;
import org.apache.flink.table.store.file.mergetree.Increment;
import org.apache.flink.table.store.file.mergetree.Levels;
import org.apache.flink.table.store.file.mergetree.MemTable;
import org.apache.flink.table.store.file.mergetree.SortBufferMemTable;
import org.apache.flink.table.store.file.mergetree.compact.CompactManager;
import org.apache.flink.table.store.file.mergetree.compact.DeduplicateMergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunction;
import org.apache.flink.table.store.file.stats.BinaryTableStats;
import org.apache.flink.table.store.file.stats.FieldStatsArraySerializer;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A {@link RecordWriter} implementation that only accepts records which are always insert
 * operations and don't have any unique keys or sort keys.
 */
public class AppendOnlyWriter implements RecordWriter {
    private final long targetFileSize;
    private final DataFilePathFactory pathFactory;
    private final FileStorePathFactory fileStorePathFactory;
    private final FieldStatsArraySerializer statsArraySerializer;
    private final CompactManager compactManager;
    private final Levels levels;
    private final LinkedHashMap<String, DataFileMeta> compactBefore;
    private final LinkedHashSet<DataFileMeta> compactAfter;
    private final DataFileWriter dataFileWriter;
    private final LinkedHashSet<DataFileMeta> newFiles;
    private final MemTable memTable;
    private final boolean commitForceCompact;
    private final int numSortedRunStopTrigger;
    private final Comparator<RowData> keyComparator;
    private final MergeFunction mergeFunction;
    private final FileWriter.Factory<RowData, Metric> fileWriterFactory;
    private long nextSeqNum;
    private boolean isCompactEnabled;

    private RowRollingWriter writer;
    
    public AppendOnlyWriter(
        FileFormat fileFormat,
        long targetFileSize,
        RowType writeSchema,
        long maxWroteSeqNumber,
        DataFilePathFactory pathFactory) {
        this(fileFormat,targetFileSize, writeSchema, maxWroteSeqNumber,pathFactory,new FileStorePathFactory(pathFactory.newPath()),
            null,null,null,null,null,false,0,null,false);
    }
    
    public AppendOnlyWriter(
            FileFormat fileFormat,
            long targetFileSize,
            RowType writeSchema,
            long maxWroteSeqNumber,
            DataFilePathFactory pathFactory,
            FileStorePathFactory fileStorePathFactory,
            CompactManager compactManager,
            Levels levels,
            DataFileWriter dataFileWriter,
            MemTable memTable,
            Comparator<RowData> keyComparator,
            boolean commitForceCompact,
            int numSortedRunStopTrigger,
            MergeFunction mergeFunction,
            boolean isCompactEnabled) {

        this.targetFileSize = targetFileSize;
        this.pathFactory = pathFactory;
        this.fileStorePathFactory = fileStorePathFactory;
        this.statsArraySerializer = new FieldStatsArraySerializer(writeSchema);

        // Initialize the file writer factory to write records and generic metric.
        this.fileWriterFactory =
                MetricFileWriter.createFactory(
                        fileFormat.createWriterFactory(writeSchema),
                        Function.identity(),
                        writeSchema,
                        fileFormat.createStatsExtractor(writeSchema).orElse(null));

        this.nextSeqNum = maxWroteSeqNumber;
        this.writer = createRollingRowWriter();
        this.compactManager = compactManager;
        this.levels = levels;
        this.compactBefore = new LinkedHashMap<>();
        this.compactAfter = new LinkedHashSet<>();
        this.dataFileWriter = dataFileWriter;
        this.keyComparator = keyComparator;
        this.newFiles = new LinkedHashSet<>();
        this.memTable = memTable;
        this.commitForceCompact = commitForceCompact;
        this.numSortedRunStopTrigger = numSortedRunStopTrigger;
        this.mergeFunction = mergeFunction;
        this.isCompactEnabled = isCompactEnabled;
    }
    
    private long newSequenceNumber() {
        return nextSeqNum++;
    }
    
    
    @Override
    public void write(ValueKind valueKind, RowData key, RowData value) throws Exception {
        Preconditions.checkArgument(
                valueKind == ValueKind.ADD,
                "Append-only writer cannot accept ValueKind: %s",
                valueKind);
    
        //if not enable compact or keep rolling write
        if (!isCompactEnabled) {
            writer.write(value);
        }
    
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
    
    @VisibleForTesting
    Levels levels() {
        return levels;
    }

    @Override
    public void sync() throws Exception {
        finishCompaction(true);
    }
    
    private void updateCompactResult(CompactManager.CompactResult result) {
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
                    dataFileWriter.delete(file);
                }
            } else {
                compactBefore.put(file.fileName(), file);
            }
        }
        compactAfter.addAll(result.after());
    }
    
    private void flush() throws Exception {
        if (memTable.size() > 0) {
            if (levels.numberOfSortedRuns() > numSortedRunStopTrigger) {
                // stop writing, wait for compaction finished
                finishCompaction(true);
            }
            Iterator<KeyValue> iterator = memTable.iterator(keyComparator, mergeFunction);
            List<DataFileMeta> files =
                dataFileWriter.write(CloseableIterator.adapterForIterator(iterator), 0);
            newFiles.addAll(files);
            files.forEach(levels::addLevel0File);
            memTable.clear();
            submitCompaction();
        }
    }
    
    
    
    private void submitCompaction() throws Exception {
        finishCompaction(false);
        if (compactManager.isCompactionFinished()) {
            compactManager.submitCompaction(levels);
        }
    }
    
    private void finishCompaction(boolean blocking) throws Exception {
        Optional<CompactManager.CompactResult> result =
            compactManager.finishCompaction(levels, blocking);
        result.ifPresent(this::updateCompactResult);
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
                    Math.max(minSeqNum, nextSeqNum - 1));
        }
    }
}
