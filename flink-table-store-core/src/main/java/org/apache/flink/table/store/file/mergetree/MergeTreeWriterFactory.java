/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.file.mergetree;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.FileFormat;
import org.apache.flink.table.store.file.mergetree.compact.Accumulator;
import org.apache.flink.table.store.file.mergetree.compact.CompactManager;
import org.apache.flink.table.store.file.mergetree.compact.CompactStrategy;
import org.apache.flink.table.store.file.mergetree.compact.UniversalCompaction;
import org.apache.flink.table.store.file.mergetree.sst.SstFileMeta;
import org.apache.flink.table.store.file.mergetree.sst.SstFileWriter;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.RecordReaderIterator;
import org.apache.flink.table.store.file.utils.RecordWriter;
import org.apache.flink.table.types.logical.RowType;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;

/** Creates {@link MergeTreeWriter}. */
public class MergeTreeWriterFactory {

    private final MergeTreeReaderFactory mergeTreeReaderFactory;
    private final SstFileWriter.Factory sstFileWriterFactory;
    private final Comparator<RowData> keyComparator;
    private final Accumulator accumulator;
    private final MergeTreeOptions options;

    public MergeTreeWriterFactory(
            RowType keyType,
            RowType valueType,
            Comparator<RowData> keyComparator,
            Accumulator accumulator,
            FileFormat fileFormat,
            FileStorePathFactory pathFactory,
            MergeTreeOptions options) {
        this.mergeTreeReaderFactory =
                new MergeTreeReaderFactory(
                        keyType, valueType, keyComparator, accumulator, fileFormat, pathFactory);
        this.sstFileWriterFactory =
                new SstFileWriter.Factory(
                        keyType, valueType, fileFormat, pathFactory, options.targetFileSize);
        this.keyComparator = keyComparator;
        this.accumulator = accumulator;
        this.options = options;
    }

    /**
     * Create {@link RecordWriter} from restored files. Some compaction of files may occur during
     * the write process.
     */
    public RecordWriter create(
            BinaryRowData partition,
            int bucket,
            List<SstFileMeta> restoreFiles,
            ExecutorService compactExecutor) {
        long maxSequenceNumber =
                restoreFiles.stream()
                        .map(SstFileMeta::maxSequenceNumber)
                        .max(Long::compare)
                        .orElse(-1L);
        SstFileWriter sstFileWriter = sstFileWriterFactory.create(partition, bucket);
        return new MergeTreeWriter(
                new SortBufferMemTable(
                        sstFileWriter.keyType(),
                        sstFileWriter.valueType(),
                        options.writeBufferSize,
                        options.pageSize),
                createCompactManager(partition, bucket, sstFileWriter, compactExecutor),
                new Levels(keyComparator, restoreFiles, options.numLevels),
                maxSequenceNumber,
                keyComparator,
                accumulator.copy(),
                sstFileWriter,
                options.commitForceCompact);
    }

    private CompactManager createCompactManager(
            BinaryRowData partition,
            int bucket,
            SstFileWriter sstFileWriter,
            ExecutorService compactExecutor) {
        CompactStrategy compactStrategy =
                new UniversalCompaction(
                        options.maxSizeAmplificationPercent,
                        options.sizeRatio,
                        options.numSortedRunMax);
        CompactManager.Rewriter rewriter =
                (outputLevel, dropDelete, sections) ->
                        sstFileWriter.write(
                                new RecordReaderIterator(
                                        mergeTreeReaderFactory.create(
                                                partition, bucket, sections, dropDelete)),
                                outputLevel);
        return new CompactManager(
                compactExecutor, compactStrategy, keyComparator, options.targetFileSize, rewriter);
    }
}
