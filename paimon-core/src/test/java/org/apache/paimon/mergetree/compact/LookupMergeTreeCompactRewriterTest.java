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
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.FileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.mergetree.LookupLevels;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.options.Options;
import org.apache.paimon.stats.SimpleStats;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Collections;
import java.util.Comparator;

import static org.apache.paimon.mergetree.compact.ChangelogMergeTreeRewriter.UpgradeStrategy.CHANGELOG_WITH_REWRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/** Tests for {@link LookupMergeTreeCompactRewriter}. */
class LookupMergeTreeCompactRewriterTest {

    @ParameterizedTest
    @EnumSource(
            value = CoreOptions.MergeEngine.class,
            names = {"DEDUPLICATE", "PARTIAL_UPDATE"})
    void testVectorIndexForcesLevelZeroRewrite(CoreOptions.MergeEngine mergeEngine) {
        Options rawOptions = new Options();
        rawOptions.set(CoreOptions.MERGE_ENGINE, mergeEngine);
        rawOptions.set(CoreOptions.PK_VECTOR_INDEX_COLUMNS, "embedding");
        CoreOptions options = new CoreOptions(rawOptions);
        LookupMergeTreeCompactRewriter<Object> rewriter =
                new LookupMergeTreeCompactRewriter<>(
                        2,
                        mergeEngine,
                        mock(LookupLevels.class),
                        mock(FileReaderFactory.class),
                        mock(KeyValueFileWriterFactory.class),
                        Comparator.comparingInt(row -> row.getInt(0)),
                        null,
                        mock(MergeFunctionFactory.class),
                        mock(MergeSorter.class),
                        mock(LookupMergeTreeCompactRewriter.MergeFunctionWrapperFactory.class),
                        false,
                        null,
                        options,
                        null);

        assertThat(rewriter.upgradeStrategy(2, levelZeroFile())).isEqualTo(CHANGELOG_WITH_REWRITE);
    }

    private static DataFileMeta levelZeroFile() {
        return DataFileMeta.forAppend(
                "data-1",
                100,
                10,
                SimpleStats.EMPTY_STATS,
                0,
                0,
                1,
                Collections.emptyList(),
                null,
                FileSource.APPEND,
                null,
                null,
                null,
                null);
    }
}
