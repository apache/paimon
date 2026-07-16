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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PkSortedBucketIndexState}. */
class PkSortedBucketIndexStateTest {

    @Test
    void testRotatedPayloadsFormOneCoveredGroup() {
        PrimaryKeyIndexSourceFile source = new PrimaryKeyIndexSourceFile("data-1", 10);
        PkSortedBucketIndexState state =
                PkSortedBucketIndexState.fromActivePayloads(
                        7,
                        "btree",
                        Collections.singletonList(source),
                        Arrays.asList(
                                payload("index-1", source, "btree", 7, 0, 9, 4),
                                payload("index-2", source, "btree", 7, 0, 9, 6)));

        assertThat(state.groups()).hasSize(1);
        assertThat(state.groups().get(0).payloads())
                .extracting(IndexFileMeta::fileName)
                .containsExactly("index-1", "index-2");
        assertThat(state.coveredSourceFiles()).containsExactly(source);
        assertThat(state.uncoveredSourceFiles()).isEmpty();
        assertThat(state.rejectedPayloads()).isEmpty();
    }

    @Test
    void testMultiSourcePayloadsFormOneCoveredGroup() {
        PrimaryKeyIndexSourceFile sourceA = new PrimaryKeyIndexSourceFile("data-a", 3);
        PrimaryKeyIndexSourceFile sourceB = new PrimaryKeyIndexSourceFile("data-b", 7);
        List<PrimaryKeyIndexSourceFile> sources = Arrays.asList(sourceA, sourceB);
        PkSortedBucketIndexState state =
                PkSortedBucketIndexState.fromActivePayloads(
                        7,
                        "btree",
                        sources,
                        Arrays.asList(
                                payload("index-1", sources, "btree", 7, 0, 9, 4),
                                payload("index-2", sources, "btree", 7, 0, 9, 6)));

        assertThat(state.groups()).hasSize(1);
        assertThat(state.groups().get(0).sourceFiles()).containsExactly(sourceA, sourceB);
        assertThat(state.coveredSourceFiles()).containsExactly(sourceA, sourceB);
        assertThat(state.uncoveredSourceFiles()).isEmpty();
        assertThat(state.rejectedPayloads()).isEmpty();
    }

    @Test
    void testPartiallyStaleGroupRemainsAndCoversItsActiveSource() {
        PrimaryKeyIndexSourceFile stale = new PrimaryKeyIndexSourceFile("data-a", 3);
        PrimaryKeyIndexSourceFile active = new PrimaryKeyIndexSourceFile("data-b", 7);
        List<PrimaryKeyIndexSourceFile> sources = Arrays.asList(stale, active);

        PkSortedBucketIndexState state =
                PkSortedBucketIndexState.fromActivePayloads(
                        7,
                        "btree",
                        Collections.singletonList(active),
                        Collections.singletonList(
                                payload("index-1", sources, "btree", 7, 0, 9, 10)));

        assertThat(state.groups()).hasSize(1);
        assertThat(state.groups().get(0).sourceFiles()).containsExactly(stale, active);
        assertThat(state.coveredSourceFiles()).containsExactly(active);
        assertThat(state.uncoveredSourceFiles()).isEmpty();
        assertThat(state.rejectedPayloads()).isEmpty();
    }

    @Test
    void testOverlappingActiveSourceRejectsLaterGroup() {
        PrimaryKeyIndexSourceFile sourceA = new PrimaryKeyIndexSourceFile("data-a", 3);
        PrimaryKeyIndexSourceFile sourceB = new PrimaryKeyIndexSourceFile("data-b", 7);

        IndexFileMeta first =
                payload("index-ab", Arrays.asList(sourceA, sourceB), "btree", 7, 0, 9, 10);
        IndexFileMeta overlapping = payload("index-b", sourceB, "btree", 7, 0, 6, 7);
        PkSortedBucketIndexState state =
                PkSortedBucketIndexState.fromActivePayloads(
                        7,
                        "btree",
                        Arrays.asList(sourceA, sourceB),
                        Arrays.asList(first, overlapping));

        assertThat(state.groups()).hasSize(1);
        assertThat(state.groups().get(0).payloads()).containsExactly(first);
        assertThat(state.coveredSourceFiles()).containsExactly(sourceA, sourceB);
        assertThat(state.uncoveredSourceFiles()).isEmpty();
        assertThat(state.rejectedPayloads()).containsExactly(overlapping);
    }

    @Test
    void testDuplicateSourcesRejectWholeGroup() {
        PrimaryKeyIndexSourceFile source = new PrimaryKeyIndexSourceFile("data-a", 3);
        List<PrimaryKeyIndexSourceFile> duplicateSources = Arrays.asList(source, source);
        IndexFileMeta duplicated = payload("index-a", duplicateSources, "btree", 7, 0, 5, 6);

        PkSortedBucketIndexState state =
                PkSortedBucketIndexState.fromActivePayloads(
                        7,
                        "btree",
                        Collections.singletonList(source),
                        Collections.singletonList(duplicated));

        assertThat(state.groups()).isEmpty();
        assertThat(state.uncoveredSourceFiles()).containsExactly(source);
        assertThat(state.rejectedPayloads()).containsExactly(duplicated);
    }

    @Test
    void testSourceRowCountOverflowRejectsWholeGroup() {
        PrimaryKeyIndexSourceFile huge = new PrimaryKeyIndexSourceFile("data-a", Long.MAX_VALUE);
        PrimaryKeyIndexSourceFile extra = new PrimaryKeyIndexSourceFile("data-b", 1);
        IndexFileMeta overflowing =
                payload(
                        "index-overflow",
                        Arrays.asList(huge, extra),
                        "btree",
                        7,
                        0,
                        Long.MAX_VALUE,
                        1);

        PkSortedBucketIndexState state =
                PkSortedBucketIndexState.fromActivePayloads(
                        7,
                        "btree",
                        Arrays.asList(huge, extra),
                        Collections.singletonList(overflowing));

        assertThat(state.groups()).isEmpty();
        assertThat(state.uncoveredSourceFiles()).containsExactly(huge, extra);
        assertThat(state.rejectedPayloads()).containsExactly(overflowing);
    }

    @Test
    void testIncompletePayloadRowCountLeavesSourceUncovered() {
        PrimaryKeyIndexSourceFile source = new PrimaryKeyIndexSourceFile("data-1", 10);
        PkSortedBucketIndexState state =
                PkSortedBucketIndexState.fromActivePayloads(
                        7,
                        "btree",
                        Collections.singletonList(source),
                        Arrays.asList(
                                payload("index-1", source, "btree", 7, 0, 9, 4),
                                payload("index-2", source, "btree", 7, 0, 9, 5)));

        assertThat(state.groups()).isEmpty();
        assertThat(state.coveredSourceFiles()).isEmpty();
        assertThat(state.uncoveredSourceFiles()).containsExactly(source);
        assertThat(state.rejectedPayloads())
                .extracting(IndexFileMeta::fileName)
                .containsExactly("index-1", "index-2");
    }

    @Test
    void testWrongPayloadRangeLeavesSourceUncovered() {
        PrimaryKeyIndexSourceFile source = new PrimaryKeyIndexSourceFile("data-1", 10);
        PkSortedBucketIndexState state =
                PkSortedBucketIndexState.fromActivePayloads(
                        7,
                        "bitmap",
                        Collections.singletonList(source),
                        Arrays.asList(
                                payload("index-1", source, "bitmap", 7, 0, 9, 4),
                                payload("index-2", source, "bitmap", 7, 0, 8, 6)));

        assertThat(state.groups()).isEmpty();
        assertThat(state.coveredSourceFiles()).isEmpty();
        assertThat(state.uncoveredSourceFiles()).containsExactly(source);
    }

    @Test
    void testMixedIndexTypeLeavesSourceUncovered() {
        PrimaryKeyIndexSourceFile source = new PrimaryKeyIndexSourceFile("data-1", 10);
        PkSortedBucketIndexState state =
                PkSortedBucketIndexState.fromActivePayloads(
                        7,
                        "btree",
                        Collections.singletonList(source),
                        Arrays.asList(
                                payload("index-1", source, "btree", 7, 0, 9, 4),
                                payload("index-2", source, "bitmap", 7, 0, 9, 6)));

        assertThat(state.groups()).isEmpty();
        assertThat(state.coveredSourceFiles()).isEmpty();
        assertThat(state.uncoveredSourceFiles()).containsExactly(source);
    }

    @Test
    void testMixedFieldLeavesSourceUncovered() {
        PrimaryKeyIndexSourceFile source = new PrimaryKeyIndexSourceFile("data-1", 10);
        PkSortedBucketIndexState state =
                PkSortedBucketIndexState.fromActivePayloads(
                        7,
                        "btree",
                        Collections.singletonList(source),
                        Arrays.asList(
                                payload("index-1", source, "btree", 7, 0, 9, 4),
                                payload("index-2", source, "btree", 8, 0, 9, 6)));

        assertThat(state.groups()).isEmpty();
        assertThat(state.coveredSourceFiles()).isEmpty();
        assertThat(state.uncoveredSourceFiles()).containsExactly(source);
    }

    @Test
    void testMismatchedSourceMetadataLeavesSourceUncovered() {
        PrimaryKeyIndexSourceFile source = new PrimaryKeyIndexSourceFile("data-1", 10);
        PrimaryKeyIndexSourceFile mismatchedSource = new PrimaryKeyIndexSourceFile("data-1", 11);
        PkSortedBucketIndexState state =
                PkSortedBucketIndexState.fromActivePayloads(
                        7,
                        "btree",
                        Collections.singletonList(source),
                        Arrays.asList(
                                payload("index-1", source, "btree", 7, 0, 9, 4),
                                payload("index-2", mismatchedSource, "btree", 7, 0, 9, 6)));

        assertThat(state.groups()).isEmpty();
        assertThat(state.coveredSourceFiles()).isEmpty();
        assertThat(state.uncoveredSourceFiles()).containsExactly(source);
    }

    @Test
    void testDuplicatePayloadNameLeavesSourceUncovered() {
        PrimaryKeyIndexSourceFile source = new PrimaryKeyIndexSourceFile("data-1", 10);
        PkSortedBucketIndexState state =
                PkSortedBucketIndexState.fromActivePayloads(
                        7,
                        "btree",
                        Collections.singletonList(source),
                        Arrays.asList(
                                payload("index-1", source, "btree", 7, 0, 9, 5),
                                payload("index-1", source, "btree", 7, 0, 9, 5)));

        assertThat(state.groups()).isEmpty();
        assertThat(state.coveredSourceFiles()).isEmpty();
        assertThat(state.uncoveredSourceFiles()).containsExactly(source);
    }

    @Test
    void testMalformedSourceMetadataLeavesSourceUncovered() {
        PrimaryKeyIndexSourceFile source = new PrimaryKeyIndexSourceFile("data-1", 10);
        IndexFileMeta malformed =
                new IndexFileMeta(
                        "btree",
                        "index-1",
                        100,
                        10,
                        new GlobalIndexMeta(0, 9, 7, null, new byte[] {1}, new byte[] {1}),
                        null);

        PkSortedBucketIndexState state =
                PkSortedBucketIndexState.fromActivePayloads(
                        7,
                        "btree",
                        Collections.singletonList(source),
                        Collections.singletonList(malformed));

        assertThat(state.groups()).isEmpty();
        assertThat(state.coveredSourceFiles()).isEmpty();
        assertThat(state.uncoveredSourceFiles()).containsExactly(source);
        assertThat(state.rejectedPayloads()).containsExactly(malformed);
    }

    private static IndexFileMeta payload(
            String fileName,
            PrimaryKeyIndexSourceFile source,
            String indexType,
            int fieldId,
            long rangeStart,
            long rangeEnd,
            long rowCount) {
        return payload(
                fileName,
                Collections.singletonList(source),
                indexType,
                fieldId,
                rangeStart,
                rangeEnd,
                rowCount);
    }

    private static IndexFileMeta payload(
            String fileName,
            java.util.List<PrimaryKeyIndexSourceFile> sources,
            String indexType,
            int fieldId,
            long rangeStart,
            long rangeEnd,
            long rowCount) {
        byte[] sourceMeta = new PrimaryKeyIndexSourceMeta(sources).serialize();
        return new IndexFileMeta(
                indexType,
                fileName,
                100,
                rowCount,
                new GlobalIndexMeta(
                        rangeStart, rangeEnd, fieldId, null, new byte[] {1}, sourceMeta),
                null);
    }
}
