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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.mergetree.compact.Accumulator;
import org.apache.flink.table.store.file.mergetree.compact.CompactManager;
import org.apache.flink.table.store.file.mergetree.compact.CompactStrategy;
import org.apache.flink.table.store.file.mergetree.compact.UniversalCompaction;
import org.apache.flink.table.store.file.mergetree.sst.SstFile;
import org.apache.flink.table.store.file.mergetree.sst.SstFileMeta;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.file.utils.RecordReaderIterator;
import org.apache.flink.table.store.file.utils.RecordWriter;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;

/** A merge tree, provides writer and reader, the granularity of the change is the file. */
public class MergeTree {

    private final MergeTreeOptions options;

    private final SstFile sstFile;

    private final Comparator<RowData> keyComparator;

    private final ExecutorService compactExecutor;

    private final Accumulator accumulator;

    public MergeTree(
            MergeTreeOptions options,
            SstFile sstFile,
            Comparator<RowData> keyComparator,
            ExecutorService compactExecutor,
            Accumulator accumulator) {
        this.options = options;
        this.sstFile = sstFile;
        this.keyComparator = keyComparator;
        this.compactExecutor = compactExecutor;
        this.accumulator = accumulator;
    }

    /**
     * Create {@link RecordWriter} from restored files. Some compaction of files may occur during
     * the write process.
     */
    public RecordWriter createWriter(List<SstFileMeta> restoreFiles) {
        long maxSequenceNumber =
                restoreFiles.stream()
                        .map(SstFileMeta::maxSequenceNumber)
                        .max(Long::compare)
                        .orElse(-1L);
        return new MergeTreeWriter(
                new SortBufferMemTable(
                        sstFile.keyType(),
                        sstFile.valueType(),
                        options.writeBufferSize,
                        options.pageSize),
                createCompactManager(),
                new Levels(keyComparator, restoreFiles, options.numLevels),
                maxSequenceNumber,
                keyComparator,
                accumulator.copy(),
                sstFile,
                options.commitForceCompact);
    }

    /**
     * Create {@link RecordReader} from file sections. The caller can decide whether to drop the
     * deletion record.
     */
    public RecordReader createReader(List<List<SortedRun>> sections, boolean dropDelete)
            throws IOException {
        return new MergeTreeReader(
                sections, dropDelete, sstFile, keyComparator, accumulator.copy());
    }

    private CompactManager createCompactManager() {
        CompactStrategy compactStrategy =
                new UniversalCompaction(
                        options.maxSizeAmplificationPercent,
                        options.sizeRatio,
                        options.numSortedRunMax);
        CompactManager.Rewriter rewriter =
                (outputLevel, dropDelete, sections) ->
                        sstFile.write(
                                new RecordReaderIterator(createReader(sections, dropDelete)),
                                outputLevel);
        return new CompactManager(
                compactExecutor, compactStrategy, keyComparator, options.targetFileSize, rewriter);
    }
}
