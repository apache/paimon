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

package org.apache.paimon.index.pksorted;

import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PkSortedBucketIndexState}. */
class PkSortedBucketIndexStateTest {

    @Test
    void testAcceptsOnePayloadForCompleteLevel() {
        DataFileMeta first = dataFile("data-a", 3, 2);
        DataFileMeta second = dataFile("data-b", 7, 2);
        IndexFileMeta payload = payload("index", 2, first, second);

        PkSortedBucketIndexState state =
                PkSortedBucketIndexState.fromActiveDataFiles(
                        7,
                        "btree",
                        Arrays.asList(second, first),
                        Collections.singletonList(payload));

        assertThat(state.groups()).hasSize(1);
        assertThat(state.groups().get(0).dataLevel()).isEqualTo(2);
        assertThat(state.groups().get(0).sourceFiles())
                .extracting(PrimaryKeyIndexSourceFile::fileName)
                .containsExactly("data-a", "data-b");
        assertThat(state.coveredSourceFiles()).hasSize(2);
        assertThat(state.uncoveredSourceFiles()).isEmpty();
        assertThat(state.rejectedPayloads()).isEmpty();
    }

    @Test
    void testRejectsPartialLevelPayload() {
        DataFileMeta first = dataFile("data-a", 3, 2);
        DataFileMeta second = dataFile("data-b", 7, 2);
        IndexFileMeta partial = payload("partial", 2, first);

        PkSortedBucketIndexState state =
                PkSortedBucketIndexState.fromActiveDataFiles(
                        7,
                        "btree",
                        Arrays.asList(first, second),
                        Collections.singletonList(partial));

        assertThat(state.groups()).isEmpty();
        assertThat(state.uncoveredSourceFiles()).hasSize(2);
        assertThat(state.rejectedPayloads()).containsExactly(partial);
    }

    @Test
    void testRejectsDuplicatePayloadsForLevel() {
        DataFileMeta data = dataFile("data", 3, 2);
        IndexFileMeta first = payload("first", 2, data);
        IndexFileMeta second = payload("second", 2, data);

        PkSortedBucketIndexState state =
                PkSortedBucketIndexState.fromActiveDataFiles(
                        7, "btree", Collections.singletonList(data), Arrays.asList(first, second));

        assertThat(state.groups()).isEmpty();
        assertThat(state.rejectedPayloads()).containsExactly(first, second);
    }

    @Test
    void testRejectsPayloadForDifferentLevel() {
        DataFileMeta data = dataFile("data", 3, 2);
        IndexFileMeta wrongLevel = payload("wrong-level", 3, data);

        PkSortedBucketIndexState state =
                PkSortedBucketIndexState.fromActiveDataFiles(
                        7,
                        "btree",
                        Collections.singletonList(data),
                        Collections.singletonList(wrongLevel));

        assertThat(state.groups()).isEmpty();
        assertThat(state.rejectedPayloads()).containsExactly(wrongLevel);
    }

    @Test
    void testRejectsMalformedSourceMetadata() {
        DataFileMeta data = dataFile("data", 3, 2);
        IndexFileMeta malformed =
                new IndexFileMeta(
                        "btree",
                        "malformed",
                        100,
                        3,
                        new GlobalIndexMeta(0, 2, 7, null, new byte[] {1}, new byte[] {1}),
                        null);

        PkSortedBucketIndexState state =
                PkSortedBucketIndexState.fromActiveDataFiles(
                        7,
                        "btree",
                        Collections.singletonList(data),
                        Collections.singletonList(malformed));

        assertThat(state.groups()).isEmpty();
        assertThat(state.rejectedPayloads()).containsExactly(malformed);
    }

    private static DataFileMeta dataFile(String name, long rowCount, int level) {
        return DataFileMeta.forAppend(
                        name,
                        100,
                        rowCount,
                        SimpleStats.EMPTY_STATS,
                        0,
                        0,
                        1,
                        Collections.emptyList(),
                        null,
                        FileSource.COMPACT,
                        null,
                        null,
                        null,
                        null)
                .upgrade(level);
    }

    private static IndexFileMeta payload(String name, int level, DataFileMeta... files) {
        List<PrimaryKeyIndexSourceFile> sources =
                Arrays.asList(files).stream()
                        .sorted(java.util.Comparator.comparing(DataFileMeta::fileName))
                        .map(
                                file ->
                                        new PrimaryKeyIndexSourceFile(
                                                file.fileName(), file.rowCount()))
                        .collect(java.util.stream.Collectors.toList());
        long rowCount = 0;
        for (PrimaryKeyIndexSourceFile source : sources) {
            rowCount += source.rowCount();
        }
        return new IndexFileMeta(
                "btree",
                name,
                100,
                rowCount,
                new GlobalIndexMeta(
                        0,
                        rowCount - 1,
                        7,
                        null,
                        new byte[] {1},
                        new PrimaryKeyIndexSourceMeta(level, sources).serialize()),
                null);
    }
}
