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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.KeyValue;
import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.MergeTreeReaders;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.reader.RecordReaderIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/** A {@link MergeTreeCompactRewriter} which produces changelog files for the compaction. */
public abstract class ChangelogMergeTreeRewriter extends MergeTreeCompactRewriter {

    protected final RecordEqualiser valueEqualiser;
    protected final boolean changelogRowDeduplicate;

    public ChangelogMergeTreeRewriter(
            KeyValueFileReaderFactory readerFactory,
            KeyValueFileWriterFactory writerFactory,
            Comparator<InternalRow> keyComparator,
            MergeFunctionFactory<KeyValue> mfFactory,
            MergeSorter mergeSorter,
            RecordEqualiser valueEqualiser,
            boolean changelogRowDeduplicate) {
        super(readerFactory, writerFactory, keyComparator, mfFactory, mergeSorter);
        this.valueEqualiser = valueEqualiser;
        this.changelogRowDeduplicate = changelogRowDeduplicate;
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
                    () ->
                            MergeTreeReaders.readerForSection(
                                    section,
                                    readerFactory,
                                    keyComparator,
                                    createMergeWrapper(outputLevel),
                                    mergeSorter));
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
