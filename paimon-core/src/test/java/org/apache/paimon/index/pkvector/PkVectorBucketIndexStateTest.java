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

package org.apache.paimon.index.pkvector;

import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PkVectorBucketIndexState}. */
class PkVectorBucketIndexStateTest {

    @Test
    void testDerivesAnnCoverage() {
        IndexFileMeta ann = segment("ann", "data-1", "test-vector-ann");

        PkVectorBucketIndexState state =
                PkVectorBucketIndexState.fromActivePayloads(
                        7, "test-vector-ann", Collections.singletonList(ann));

        assertThat(state.vectorFieldId()).isEqualTo(7);
        assertThat(state.annSegments()).extracting(IndexFileMeta::fileName).containsExactly("ann");
        assertThat(state.sourceFileToAnnSegment()).containsOnlyKeys("data-1");
    }

    @Test
    void testRejectsDuplicateAnnSource() {
        assertThatThrownBy(
                        () ->
                                PkVectorBucketIndexState.fromActivePayloads(
                                        7,
                                        "test-vector-ann",
                                        java.util.Arrays.asList(
                                                segment("ann-1", "data", "test-vector-ann"),
                                                segment("ann-2", "data", "test-vector-ann"))))
                .hasMessageContaining("both ANN segments");
    }

    @Test
    void testRejectsDifferentIndexType() {
        assertThatThrownBy(
                        () ->
                                PkVectorBucketIndexState.fromActivePayloads(
                                        7,
                                        "test-vector-ann",
                                        Collections.singletonList(
                                                segment("ann", "data", "other-vector-ann"))))
                .hasMessageContaining("different index type");
    }

    @Test
    void testEmptyPayloadsProduceEmptyState() {
        PkVectorBucketIndexState state =
                PkVectorBucketIndexState.fromActivePayloads(
                        7, "test-vector-ann", Collections.emptyList());

        assertThat(state.vectorFieldId()).isEqualTo(7);
        assertThat(state.annSegments()).isEmpty();
    }

    private static IndexFileMeta segment(
            String segmentFileName, String sourceFileName, String indexType) {
        PkVectorSourceFile sourceFile = new PkVectorSourceFile(sourceFileName, 10);
        byte[] sourceMeta =
                new PkVectorSourceMeta(Collections.singletonList(sourceFile)).serialize();
        return new IndexFileMeta(
                indexType,
                segmentFileName,
                100,
                10,
                new GlobalIndexMeta(0, 10, 7, null, new byte[] {1}, sourceMeta),
                null);
    }
}
