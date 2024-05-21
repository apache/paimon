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

import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.KeyValue;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.FileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.FieldsComparator;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A {@link MergeTreeCompactRewriter} which produces changelog files while performing compaction.
 */
public abstract class ChangelogMergeTreeRewriter extends MergeTreeCompactRewriter {

    protected final int maxLevel;
    protected final MergeEngine mergeEngine;
    private final boolean produceChangelog;
    private final boolean forceDropDelete;

    public ChangelogMergeTreeRewriter(
            int maxLevel,
            MergeEngine mergeEngine,
            FileReaderFactory<KeyValue> readerFactory,
            KeyValueFileWriterFactory writerFactory,
            Comparator<InternalRow> keyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionFactory<KeyValue> mfFactory,
            MergeSorter mergeSorter,
            boolean produceChangelog,
            boolean forceDropDelete) {
        super(
                readerFactory,
                writerFactory,
                keyComparator,
                userDefinedSeqComparator,
                mfFactory,
                mergeSorter);
        this.maxLevel = maxLevel;
        this.mergeEngine = mergeEngine;
        this.produceChangelog = produceChangelog;
        this.forceDropDelete = forceDropDelete;
    }

    protected abstract boolean rewriteChangelog(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections);

    protected abstract UpgradeStrategy upgradeStrategy(int outputLevel, DataFileMeta file);

    protected abstract MergeFunctionWrapper<ChangelogResult> createMergeWrapper(int outputLevel);

    protected boolean rewriteLookupChangelog(int outputLevel, List<List<SortedRun>> sections) {
        if (outputLevel == 0) {
            return false;
        }

        for (List<SortedRun> runs : sections) {
            for (SortedRun run : runs) {
                for (DataFileMeta file : run.files()) {
                    if (file.level() == 0) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Override
    public CompactResult rewrite(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) throws Exception {
        if (rewriteChangelog(outputLevel, dropDelete, sections)) {
            return rewriteOrProduceChangelog(outputLevel, sections, dropDelete, true);
        } else {
            return rewriteCompaction(outputLevel, dropDelete, sections);
        }
    }

    /**
     * Rewrite or produce changelog at the same time.
     *
     * @param dropDelete whether to drop delete when rewrite compact file
     * @param rewriteCompactFile whether to rewrite compact file
     */
    private CompactResult rewriteOrProduceChangelog(
            int outputLevel,
            List<List<SortedRun>> sections,
            boolean dropDelete,
            boolean rewriteCompactFile)
            throws Exception {

        CloseableIterator<ChangelogResult> iterator = null;
        RollingFileWriter<KeyValue, DataFileMeta> compactFileWriter = null;
        RollingFileWriter<KeyValue, DataFileMeta> changelogFileWriter = null;

        try {
            iterator =
                    readerForMergeTree(sections, createMergeWrapper(outputLevel))
                            .toCloseableIterator();
            if (rewriteCompactFile) {
                compactFileWriter =
                        writerFactory.createRollingMergeTreeFileWriter(outputLevel, true);
            }
            if (produceChangelog) {
                changelogFileWriter = writerFactory.createRollingChangelogFileWriter(outputLevel);
            }

            while (iterator.hasNext()) {
                ChangelogResult result = iterator.next();
                KeyValue keyValue = result.result();
                if (compactFileWriter != null
                        && keyValue != null
                        && (!dropDelete || keyValue.isAdd())) {
                    compactFileWriter.write(keyValue);
                }
                if (produceChangelog) {
                    for (KeyValue kv : result.changelogs()) {
                        changelogFileWriter.write(kv);
                    }
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

        List<DataFileMeta> before = extractFilesFromSections(sections);
        List<DataFileMeta> after =
                compactFileWriter != null
                        ? compactFileWriter.result()
                        : before.stream()
                                .map(x -> x.upgrade(outputLevel))
                                .collect(Collectors.toList());

        if (rewriteCompactFile) {
            notifyRewriteCompactBefore(before);
        }

        List<DataFileMeta> changelogFiles =
                changelogFileWriter != null
                        ? changelogFileWriter.result()
                        : Collections.emptyList();
        return new CompactResult(before, after, changelogFiles);
    }

    @Override
    public CompactResult upgrade(int outputLevel, DataFileMeta file) throws Exception {
        UpgradeStrategy strategy = upgradeStrategy(outputLevel, file);
        if (strategy.changelog) {
            return rewriteOrProduceChangelog(
                    outputLevel,
                    Collections.singletonList(
                            Collections.singletonList(SortedRun.fromSingle(file))),
                    forceDropDelete,
                    strategy.rewrite);
        } else {
            return super.upgrade(outputLevel, file);
        }
    }

    /** Strategy for upgrade. */
    protected enum UpgradeStrategy {
        NO_CHANGELOG_NO_REWRITE(false, false),
        CHANGELOG_NO_REWRITE(true, false),
        CHANGELOG_WITH_REWRITE(true, true);

        private final boolean changelog;
        private final boolean rewrite;

        UpgradeStrategy(boolean changelog, boolean rewrite) {
            this.changelog = changelog;
            this.rewrite = rewrite;
        }
    }
}
