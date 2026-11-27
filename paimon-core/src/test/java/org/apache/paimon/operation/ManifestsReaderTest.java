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

package org.apache.paimon.operation;

import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.utils.RoaringNavigableMap64;
import org.apache.paimon.utils.RowRangeIndex;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ManifestsReader}. */
class ManifestsReaderTest {

    @Test
    void testPruneKeepsTransitivelyOverlappingManifestsInOriginalOrder() {
        ManifestFileMeta tail = manifest("tail", 18L, 30L);
        ManifestFileMeta adjacent = manifest("adjacent", 31L, 40L);
        ManifestFileMeta head = manifest("head", 0L, 10L);
        ManifestFileMeta bridge = manifest("bridge", 10L, 18L);
        ManifestFileMeta distant = manifest("distant", 100L, 200L);
        List<ManifestFileMeta> manifests = Arrays.asList(tail, adjacent, head, distant, bridge);

        assertThat(pruneManifests(manifests, RoaringNavigableMap64.bitmapOf(5, 200)))
                .containsExactly(tail, head, distant, bridge);
    }

    @Test
    void testPruneKeepsAllManifestsIfBoundsAreMissing() {
        ManifestFileMeta known = manifest("known", 30L, 40L);
        ManifestFileMeta unknown = manifest("unknown", null, null);
        List<ManifestFileMeta> manifests = Arrays.asList(known, unknown);

        assertThat(pruneManifests(manifests, RoaringNavigableMap64.bitmapOf(5)))
                .containsExactly(known, unknown);
    }

    @Test
    void testPruneKeepsDeleteOnlyManifestForSelectedAdds() {
        ManifestFileMeta left = manifest("left", 0L, 10L);
        ManifestFileMeta right = manifest("right", 80L, 90L);
        ManifestFileMeta spanningDelete = manifest("spanning-delete", 8L, 85L, 0, 2);
        ManifestFileMeta disjointDelete = manifest("disjoint-delete", 100L, 120L, 0, 1);
        List<ManifestFileMeta> manifests =
                Arrays.asList(right, spanningDelete, left, disjointDelete);

        assertThat(pruneManifests(manifests, RoaringNavigableMap64.bitmapOf(5)))
                .containsExactly(spanningDelete, left);
        assertThat(pruneManifests(manifests, RoaringNavigableMap64.bitmapOf(5, 85)))
                .containsExactly(right, spanningDelete, left);
    }

    @Test
    void testPruneKeepsDeleteWithoutBoundsForSelectedAdds() {
        ManifestFileMeta skipped = manifest("skipped", 30L, 40L);
        ManifestFileMeta unknownDelete = manifest("unknown-delete", null, null, 0, 1);
        ManifestFileMeta selected = manifest("selected", 0L, 10L);
        List<ManifestFileMeta> manifests = Arrays.asList(skipped, unknownDelete, selected);

        assertThat(pruneManifests(manifests, RoaringNavigableMap64.bitmapOf(5)))
                .containsExactly(unknownDelete, selected);
        assertThat(pruneManifests(manifests, RoaringNavigableMap64.bitmapOf(20))).isEmpty();
    }

    private static List<ManifestFileMeta> pruneManifests(
            List<ManifestFileMeta> manifests, RoaringNavigableMap64 candidates) {
        RowRangeIndex retainedGroups =
                ManifestsReader.selectManifestGroupRanges(
                        manifests, candidates, Collections.emptyMap());
        return ManifestsReader.filterManifestsByRowRanges(manifests, retainedGroups);
    }

    private static ManifestFileMeta manifest(String name, Long minRowId, Long maxRowId) {
        return manifest(name, minRowId, maxRowId, 1, 0);
    }

    private static ManifestFileMeta manifest(
            String name, Long minRowId, Long maxRowId, long added, long deleted) {
        return new ManifestFileMeta(
                name,
                100,
                added,
                deleted,
                SimpleStats.EMPTY_STATS,
                0,
                null,
                null,
                null,
                null,
                minRowId,
                maxRowId,
                null,
                null);
    }
}
