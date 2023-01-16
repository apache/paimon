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

package org.apache.flink.table.store.file.mergetree.compact;

import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.compact.CompactResult;
import org.apache.flink.table.store.file.io.DataFileMeta;
import org.apache.flink.table.store.file.io.KeyValueFileReaderFactory;
import org.apache.flink.table.store.file.io.KeyValueFileWriterFactory;
import org.apache.flink.table.store.file.io.RollingFileWriter;
import org.apache.flink.table.store.file.mergetree.MergeTreeReaders;
import org.apache.flink.table.store.file.mergetree.SortedRun;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.file.utils.RecordReaderIterator;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/** A {@link MergeTreeCompactRewriter} which produces changelog files for each full compaction. */
public class FullChangelogMergeTreeCompactRewriter extends MergeTreeCompactRewriter {

    private final int maxLevel;

    public FullChangelogMergeTreeCompactRewriter(
            int maxLevel,
            KeyValueFileReaderFactory readerFactory,
            KeyValueFileWriterFactory writerFactory,
            Comparator<InternalRow> keyComparator,
            MergeFunctionFactory<KeyValue> mfFactory) {
        super(readerFactory, writerFactory, keyComparator, mfFactory);
        this.maxLevel = maxLevel;
    }

    @Override
    public CompactResult rewrite(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) throws Exception {
        if (outputLevel == maxLevel) {
            Preconditions.checkArgument(
                    dropDelete,
                    "Delete records should be dropped from result of full compaction. This is unexpected.");
            return rewriteFullCompaction(sections);
        } else {
            return rewriteCompaction(outputLevel, dropDelete, sections);
        }
    }

    private CompactResult rewriteFullCompaction(List<List<SortedRun>> sections) throws Exception {
        List<ConcatRecordReader.ReaderSupplier<FullChangelogMergeFunctionWrapper.Result>>
                sectionReaders = new ArrayList<>();
        for (List<SortedRun> section : sections) {
            sectionReaders.add(
                    () -> {
                        List<RecordReader<KeyValue>> runReaders = new ArrayList<>();
                        for (SortedRun run : section) {
                            runReaders.add(MergeTreeReaders.readerForRun(run, readerFactory));
                        }
                        return new SortMergeReader<>(
                                runReaders,
                                keyComparator,
                                new FullChangelogMergeFunctionWrapper(
                                        mfFactory.create(), maxLevel));
                    });
        }

        RecordReaderIterator<FullChangelogMergeFunctionWrapper.Result> iterator = null;
        RollingFileWriter<KeyValue, DataFileMeta> compactFileWriter = null;
        RollingFileWriter<KeyValue, DataFileMeta> changelogFileWriter = null;

        try {
            iterator = new RecordReaderIterator<>(ConcatRecordReader.create(sectionReaders));
            compactFileWriter = writerFactory.createRollingMergeTreeFileWriter(maxLevel);
            changelogFileWriter = writerFactory.createRollingChangelogFileWriter(maxLevel);

            while (iterator.hasNext()) {
                FullChangelogMergeFunctionWrapper.Result result = iterator.next();
                if (result.result() != null) {
                    compactFileWriter.write(result.result());
                }
                if (result.before() != null) {
                    changelogFileWriter.write(result.before());
                }
                if (result.after() != null) {
                    changelogFileWriter.write(result.after());
                }
            }
        } finally {
            if (iterator != null) {
                iterator.close();
            }
            if (compactFileWriter != null) {
                compactFileWriter.close();
            }
            if (changelogFileWriter != null) {
                changelogFileWriter.close();
            }
        }

        return new CompactResult(
                extractFilesFromSections(sections),
                compactFileWriter.result(),
                changelogFileWriter.result());
    }

    @Override
    public CompactResult upgrade(int outputLevel, DataFileMeta file) throws Exception {
        if (outputLevel == maxLevel) {
            return rewriteFullCompaction(
                    Collections.singletonList(
                            Collections.singletonList(SortedRun.fromSingle(file))));
        } else {
            return super.upgrade(outputLevel, file);
        }
    }
}
