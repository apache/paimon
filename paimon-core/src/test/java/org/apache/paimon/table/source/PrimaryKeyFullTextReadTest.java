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

package org.apache.paimon.table.source;

import org.apache.paimon.CoreOptions.GlobalIndexSearchMode;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

/** Tests PK full-text search modes and physical scored results. */
class PrimaryKeyFullTextReadTest {

    @Test
    void testFastSkipsRawFallbackAndPropagatesScores() {
        AtomicInteger rawCalls = new AtomicInteger();
        PrimaryKeyFullTextRead read =
                new PrimaryKeyFullTextRead(
                        GlobalIndexSearchMode.FAST,
                        10,
                        split ->
                                Collections.singletonList(
                                        Collections.singletonList(position("indexed", 1, 9F))),
                        split -> {
                            rawCalls.incrementAndGet();
                            return Collections.singletonList(
                                    Collections.singletonList(position("raw", 0, 8F)));
                        });

        PrimaryKeyScoredResult result = read.read(Collections.singletonList(split()));

        assertThat(rawCalls).hasValue(0);
        assertThat(result.snapshotId()).isEqualTo(11);
        assertThat(result.positions())
                .extracting(
                        PrimaryKeySearchPosition::dataFileName,
                        PrimaryKeySearchPosition::rowPosition)
                .containsExactly(tuple("indexed", 1L));
        assertThat(result.positions().get(0).score()).isEqualTo(1F / 61F);
        IndexedSplit indexedSplit = result.splits().get(0);
        assertThat(indexedSplit.scores()).containsExactly(1F / 61F);
    }

    @ParameterizedTest
    @EnumSource(
            value = GlobalIndexSearchMode.class,
            names = {"FULL", "DETAIL"})
    void testCompleteModesSearchAndCleanRawFallback(GlobalIndexSearchMode mode) {
        AtomicInteger rawCalls = new AtomicInteger();
        AtomicInteger cleanups = new AtomicInteger();
        PrimaryKeyFullTextRead read =
                new PrimaryKeyFullTextRead(
                        mode,
                        10,
                        split ->
                                Collections.singletonList(
                                        Collections.singletonList(position("indexed", 1, 9F))),
                        split -> {
                            rawCalls.incrementAndGet();
                            try (Cleanup ignored = new Cleanup(cleanups)) {
                                return Collections.singletonList(
                                        Collections.singletonList(position("raw", 0, 8F)));
                            }
                        });

        PrimaryKeyScoredResult result = read.read(Collections.singletonList(split()));

        assertThat(rawCalls).hasValue(1);
        assertThat(cleanups).hasValue(1);
        assertThat(result.positions())
                .extracting(
                        PrimaryKeySearchPosition::dataFileName,
                        PrimaryKeySearchPosition::rowPosition)
                .containsExactly(tuple("indexed", 1L), tuple("raw", 0L));
        assertThat(result.positions())
                .allSatisfy(position -> assertThat(position.score()).isEqualTo(1F / 61F));
    }

    private static PrimaryKeySearchPosition position(
            String dataFile, long rowPosition, float score) {
        return new PrimaryKeySearchPosition(BinaryRow.EMPTY_ROW, 0, dataFile, rowPosition, score);
    }

    private static PrimaryKeyFullTextSearchSplit split() {
        List<DataFileMeta> dataFiles = Arrays.asList(dataFile("indexed"), dataFile("raw"));
        DataSplit dataSplit =
                DataSplit.builder()
                        .withSnapshot(11)
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withTotalBuckets(1)
                        .withDataFiles(dataFiles)
                        .build();
        return new PrimaryKeyFullTextSearchSplit(
                dataSplit,
                Collections.singletonList(payload("indexed")),
                Collections.singletonList("raw"));
    }

    private static DataFileMeta dataFile(String name) {
        return DataFileMeta.create(
                name,
                100,
                2,
                BinaryRow.EMPTY_ROW,
                BinaryRow.EMPTY_ROW,
                SimpleStats.EMPTY_STATS,
                SimpleStats.EMPTY_STATS,
                0,
                0,
                0,
                1,
                Collections.emptyList(),
                0L,
                null,
                FileSource.COMPACT,
                null,
                null,
                null,
                null);
    }

    private static IndexFileMeta payload(String source) {
        byte[] sourceMeta =
                new PrimaryKeyIndexSourceMeta(
                                new PrimaryKeyIndexSourceFile(source, 2), "fingerprint")
                        .serialize();
        return new IndexFileMeta(
                "full-text",
                "index-" + source,
                100,
                2,
                new GlobalIndexMeta(0, 1, 7, null, null, sourceMeta),
                null);
    }

    private static class Cleanup implements AutoCloseable {

        private final AtomicInteger cleanups;

        private Cleanup(AtomicInteger cleanups) {
            this.cleanups = cleanups;
        }

        @Override
        public void close() {
            cleanups.incrementAndGet();
        }
    }
}
