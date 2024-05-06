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
import org.apache.paimon.deletionvectors.DeletionVectorsMaintainer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.FileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.lookup.LookupStrategy;
import org.apache.paimon.mergetree.LookupLevels;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.UserDefinedSeqComparator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.List;

import static org.apache.paimon.mergetree.compact.ChangelogMergeTreeRewriter.UpgradeStrategy.CHANGELOG_NO_REWRITE;
import static org.apache.paimon.mergetree.compact.ChangelogMergeTreeRewriter.UpgradeStrategy.CHANGELOG_WITH_REWRITE;
import static org.apache.paimon.mergetree.compact.ChangelogMergeTreeRewriter.UpgradeStrategy.NO_CHANGELOG_NO_REWRITE;

/**
 * A {@link MergeTreeCompactRewriter} which produces changelog files by lookup for the compaction
 * involving level 0 files.
 */
public class LookupMergeTreeCompactRewriter<T> extends ChangelogMergeTreeRewriter {

    private final LookupLevels<T> lookupLevels;
    private final MergeFunctionWrapperFactory<T> wrapperFactory;
    @Nullable private final DeletionVectorsMaintainer dvMaintainer;

    public LookupMergeTreeCompactRewriter(
            int maxLevel,
            MergeEngine mergeEngine,
            LookupLevels<T> lookupLevels,
            FileReaderFactory<KeyValue> readerFactory,
            KeyValueFileWriterFactory writerFactory,
            Comparator<InternalRow> keyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionFactory<KeyValue> mfFactory,
            MergeSorter mergeSorter,
            MergeFunctionWrapperFactory<T> wrapperFactory,
            boolean produceChangelog,
            @Nullable DeletionVectorsMaintainer dvMaintainer) {
        super(
                maxLevel,
                mergeEngine,
                readerFactory,
                writerFactory,
                keyComparator,
                userDefinedSeqComparator,
                mfFactory,
                mergeSorter,
                produceChangelog,
                dvMaintainer != null);
        this.dvMaintainer = dvMaintainer;
        this.lookupLevels = lookupLevels;
        this.wrapperFactory = wrapperFactory;
    }

    @Override
    protected void notifyRewriteCompactBefore(List<DataFileMeta> files) {
        if (dvMaintainer != null) {
            files.forEach(file -> dvMaintainer.removeDeletionVectorOf(file.fileName()));
        }
    }

    @Override
    protected boolean rewriteChangelog(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) {
        return rewriteLookupChangelog(outputLevel, sections);
    }

    @Override
    protected UpgradeStrategy upgradeStrategy(int outputLevel, DataFileMeta file) {
        if (file.level() != 0) {
            return NO_CHANGELOG_NO_REWRITE;
        }

        // In deletionVector mode, since drop delete is required, when delete row count > 0 rewrite
        // is required.
        if (dvMaintainer != null && file.deleteRowCount().map(cnt -> cnt > 0).orElse(true)) {
            return CHANGELOG_WITH_REWRITE;
        }

        if (outputLevel == maxLevel) {
            return CHANGELOG_NO_REWRITE;
        }

        // DEDUPLICATE retains the latest records as the final result, so merging has no impact on
        // it at all.
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
        return wrapperFactory.create(mfFactory, outputLevel, lookupLevels, dvMaintainer);
    }

    @Override
    public void close() throws IOException {
        lookupLevels.close();
    }

    /** Factory to create {@link MergeFunctionWrapper}. */
    public interface MergeFunctionWrapperFactory<T> {

        MergeFunctionWrapper<ChangelogResult> create(
                MergeFunctionFactory<KeyValue> mfFactory,
                int outputLevel,
                LookupLevels<T> lookupLevels,
                @Nullable DeletionVectorsMaintainer deletionVectorsMaintainer);
    }

    /** A normal {@link MergeFunctionWrapperFactory} to create lookup wrapper. */
    public static class LookupMergeFunctionWrapperFactory<T>
            implements MergeFunctionWrapperFactory<T> {

        private final RecordEqualiser valueEqualiser;
        private final boolean changelogRowDeduplicate;
        private final LookupStrategy lookupStrategy;
        @Nullable private final UserDefinedSeqComparator userDefinedSeqComparator;

        public LookupMergeFunctionWrapperFactory(
                RecordEqualiser valueEqualiser,
                boolean changelogRowDeduplicate,
                LookupStrategy lookupStrategy,
                @Nullable UserDefinedSeqComparator userDefinedSeqComparator) {
            this.valueEqualiser = valueEqualiser;
            this.changelogRowDeduplicate = changelogRowDeduplicate;
            this.lookupStrategy = lookupStrategy;
            this.userDefinedSeqComparator = userDefinedSeqComparator;
        }

        @Override
        public MergeFunctionWrapper<ChangelogResult> create(
                MergeFunctionFactory<KeyValue> mfFactory,
                int outputLevel,
                LookupLevels<T> lookupLevels,
                @Nullable DeletionVectorsMaintainer deletionVectorsMaintainer) {
            return new LookupChangelogMergeFunctionWrapper<>(
                    mfFactory,
                    key -> {
                        try {
                            return lookupLevels.lookup(key, outputLevel + 1);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    },
                    valueEqualiser,
                    changelogRowDeduplicate,
                    lookupStrategy,
                    deletionVectorsMaintainer,
                    userDefinedSeqComparator);
        }
    }

    /** A {@link MergeFunctionWrapperFactory} for first row. */
    public static class FirstRowMergeFunctionWrapperFactory
            implements MergeFunctionWrapperFactory<Boolean> {

        @Override
        public MergeFunctionWrapper<ChangelogResult> create(
                MergeFunctionFactory<KeyValue> mfFactory,
                int outputLevel,
                LookupLevels<Boolean> lookupLevels,
                @Nullable DeletionVectorsMaintainer deletionVectorsMaintainer) {
            return new FirstRowMergeFunctionWrapper(
                    mfFactory,
                    key -> {
                        try {
                            return lookupLevels.lookup(key, outputLevel + 1) != null;
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        }
    }
}
