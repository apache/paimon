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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
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

        assertThat(
                        ManifestsReader.pruneManifestGroups(
                                manifests, RoaringNavigableMap64.bitmapOf(5, 200)))
                .containsExactly(tail, head, distant, bridge);
    }

    @Test
    void testPruneKeepsAllManifestsIfBoundsAreMissing() {
        ManifestFileMeta known = manifest("known", 30L, 40L);
        ManifestFileMeta unknown = manifest("unknown", null, null);
        List<ManifestFileMeta> manifests = Arrays.asList(known, unknown);

        assertThat(
                        ManifestsReader.pruneManifestGroups(
                                manifests, RoaringNavigableMap64.bitmapOf(5)))
                .containsExactly(known, unknown);
    }

    private static ManifestFileMeta manifest(String name, Long minRowId, Long maxRowId) {
        return new ManifestFileMeta(
                name,
                100,
                1,
                0,
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
