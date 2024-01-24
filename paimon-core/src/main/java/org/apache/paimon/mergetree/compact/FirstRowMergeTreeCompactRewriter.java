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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.mergetree.ContainsLevels;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.utils.Filter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.List;

import static org.apache.paimon.mergetree.compact.ChangelogMergeTreeRewriter.UpgradeStrategy.CHANGELOG_NO_REWRITE;
import static org.apache.paimon.mergetree.compact.ChangelogMergeTreeRewriter.UpgradeStrategy.CHANGELOG_WITH_REWRITE;
import static org.apache.paimon.mergetree.compact.ChangelogMergeTreeRewriter.UpgradeStrategy.NO_CHANGELOG;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * A {@link MergeTreeCompactRewriter} for first row merge engine which produces changelog files by
 * contains for the compaction involving level 0 files.
 */
public class FirstRowMergeTreeCompactRewriter extends ChangelogMergeTreeRewriter {

    private final ContainsLevels containsLevels;

    public FirstRowMergeTreeCompactRewriter(
            int maxLevel,
            CoreOptions.MergeEngine mergeEngine,
            ContainsLevels containsLevels,
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
        this.containsLevels = containsLevels;
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

        // FIRST_ROW must rewrite file, because some records that are already at higher level may be
        // skipped
        // See LookupMergeFunction, it just returns newly records.
        return CHANGELOG_WITH_REWRITE;
    }

    @Override
    protected MergeFunctionWrapper<ChangelogResult> createMergeWrapper(int outputLevel) {
        return new FistRowMergeFunctionWrapper(
                mfFactory,
                key -> {
                    try {
                        return containsLevels.contains(key, outputLevel + 1);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
    }

    @Override
    public void close() throws IOException {
        containsLevels.close();
    }

    @VisibleForTesting
    static class FistRowMergeFunctionWrapper implements MergeFunctionWrapper<ChangelogResult> {

        private final Filter<InternalRow> contains;
        private final FirstRowMergeFunction mergeFunction;
        private final ChangelogResult reusedResult = new ChangelogResult();

        public FistRowMergeFunctionWrapper(
                MergeFunctionFactory<KeyValue> mergeFunctionFactory, Filter<InternalRow> contains) {
            this.contains = contains;
            MergeFunction<KeyValue> mergeFunction = mergeFunctionFactory.create();
            checkArgument(
                    mergeFunction instanceof FirstRowMergeFunction,
                    "Merge function should be a FirstRowMergeFunction, but is %s, there is a bug.",
                    mergeFunction.getClass().getName());
            this.mergeFunction = (FirstRowMergeFunction) mergeFunction;
        }

        @Override
        public void reset() {
            mergeFunction.reset();
        }

        @Override
        public void add(KeyValue kv) {
            mergeFunction.add(kv);
        }

        @Override
        public ChangelogResult getResult() {
            reusedResult.reset();
            KeyValue result = mergeFunction.getResult();
            if (result == null) {
                return reusedResult;
            }

            if (contains.test(result.key())) {
                // empty
                return reusedResult;
            }

            reusedResult.setResult(result);
            if (result.level() == 0) {
                // new record, output changelog
                return reusedResult.addChangelog(result);
            }
            return reusedResult;
        }
    }
}
