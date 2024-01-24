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
import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.mergetree.LookupLevels;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.SortedRun;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.List;

import static org.apache.paimon.mergetree.compact.ChangelogMergeTreeRewriter.UpgradeStrategy.CHANGELOG_NO_REWRITE;
import static org.apache.paimon.mergetree.compact.ChangelogMergeTreeRewriter.UpgradeStrategy.CHANGELOG_WITH_REWRITE;
import static org.apache.paimon.mergetree.compact.ChangelogMergeTreeRewriter.UpgradeStrategy.NO_CHANGELOG;

/**
 * A {@link MergeTreeCompactRewriter} which produces changelog files by lookup for the compaction
 * involving level 0 files.
 */
public class LookupMergeTreeCompactRewriter extends ChangelogMergeTreeRewriter {

    private final LookupLevels lookupLevels;

    public LookupMergeTreeCompactRewriter(
            int maxLevel,
            MergeEngine mergeEngine,
            LookupLevels lookupLevels,
            KeyValueFileReaderFactory readerFactory,
            KeyValueFileWriterFactory writerFactory,
            Comparator<InternalRow> keyComparator,
            MergeFunctionFactory<KeyValue> mfFactory,
            MergeSorter mergeSorter,
            RecordEqualiser valueEqualiser,
            boolean changelogRowDeduplicate) {
        super(
                maxLevel,
                mergeEngine,
                readerFactory,
                writerFactory,
                keyComparator,
                mfFactory,
                mergeSorter,
                valueEqualiser,
                changelogRowDeduplicate);
        this.lookupLevels = lookupLevels;
    }

    @Override
    protected boolean rewriteChangelog(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) {
        return rewriteLookupChangelog(outputLevel, sections);
    }

    @Override
    protected UpgradeStrategy upgradeChangelog(int outputLevel, DataFileMeta file) {
        if (file.level() != 0) {
            return NO_CHANGELOG;
        }

        if (outputLevel == maxLevel) {
            return CHANGELOG_NO_REWRITE;
        }

        // DEDUPLICATE retains the latest records as the final result, so merging has no impact on
        // it at all
        if (mergeEngine == MergeEngine.DEDUPLICATE) {
            return CHANGELOG_NO_REWRITE;
        }

        // other merge engines must rewrite file, because some records that are already at higher
        // level may be merged
        // See LookupMergeFunction, it just returns newly records.
        return CHANGELOG_WITH_REWRITE;
    }

    @Override
    protected MergeFunctionWrapper<ChangelogResult> createMergeWrapper(int outputLevel) {
        return new LookupChangelogMergeFunctionWrapper(
                mfFactory,
                key -> {
                    try {
                        return lookupLevels.lookup(key, outputLevel + 1);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                },
                valueEqualiser,
                changelogRowDeduplicate);
    }

    @Override
    public void close() throws IOException {
        lookupLevels.close();
    }
}
