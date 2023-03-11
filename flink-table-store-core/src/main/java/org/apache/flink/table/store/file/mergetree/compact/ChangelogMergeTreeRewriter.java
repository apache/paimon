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
import org.apache.flink.table.store.reader.RecordReader;
import org.apache.flink.table.store.reader.RecordReaderIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/** A {@link MergeTreeCompactRewriter} which produces changelog files for the compaction. */
public abstract class ChangelogMergeTreeRewriter extends MergeTreeCompactRewriter {

    public ChangelogMergeTreeRewriter(
            KeyValueFileReaderFactory readerFactory,
            KeyValueFileWriterFactory writerFactory,
            Comparator<InternalRow> keyComparator,
            MergeFunctionFactory<KeyValue> mfFactory) {
        super(readerFactory, writerFactory, keyComparator, mfFactory);
    }

    protected abstract boolean rewriteChangelog(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections);

    protected abstract boolean upgradeChangelog(int outputLevel, DataFileMeta file);

    protected abstract MergeFunctionWrapper<ChangelogResult> createMergeWrapper(int outputLevel);

    @Override
    public CompactResult rewrite(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) throws Exception {
        if (rewriteChangelog(outputLevel, dropDelete, sections)) {
            return rewriteChangelogCompaction(outputLevel, sections);
        } else {
            return rewriteCompaction(outputLevel, dropDelete, sections);
        }
    }

    private CompactResult rewriteChangelogCompaction(
            int outputLevel, List<List<SortedRun>> sections) throws Exception {
        List<ConcatRecordReader.ReaderSupplier<ChangelogResult>> sectionReaders = new ArrayList<>();
        for (List<SortedRun> section : sections) {
            sectionReaders.add(
                    () -> {
                        List<RecordReader<KeyValue>> runReaders = new ArrayList<>();
                        for (SortedRun run : section) {
                            runReaders.add(MergeTreeReaders.readerForRun(run, readerFactory));
                        }
                        return new SortMergeReader<>(
                                runReaders, keyComparator, createMergeWrapper(outputLevel));
                    });
        }

        RecordReaderIterator<ChangelogResult> iterator = null;
        RollingFileWriter<KeyValue, DataFileMeta> compactFileWriter = null;
        RollingFileWriter<KeyValue, DataFileMeta> changelogFileWriter = null;

        try {
            iterator = new RecordReaderIterator<>(ConcatRecordReader.create(sectionReaders));
            compactFileWriter = writerFactory.createRollingMergeTreeFileWriter(outputLevel);
            changelogFileWriter = writerFactory.createRollingChangelogFileWriter(outputLevel);

            while (iterator.hasNext()) {
                ChangelogResult result = iterator.next();
                if (result.result() != null) {
                    compactFileWriter.write(result.result());
                }
                for (KeyValue kv : result.changelogs()) {
                    changelogFileWriter.write(kv);
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
        if (upgradeChangelog(outputLevel, file)) {
            return rewriteChangelogCompaction(
                    outputLevel,
                    Collections.singletonList(
                            Collections.singletonList(SortedRun.fromSingle(file))));
        } else {
            return super.upgrade(outputLevel, file);
        }
    }
}
