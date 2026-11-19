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

package org.apache.paimon.manifest;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.utils.Range;
import org.apache.paimon.vector.index.VectorIndexShardMeta;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Atomic manifest replacement for each registered centroid index type. */
class CentroidIndexManifestTest {
    @ParameterizedTest
    @ValueSource(strings = {"ivf-flat", "ivf-pq", "ivf-sq", "ivf-rq", "diskann"})
    void testCentroidModelsMustBeCompleteAndReplacedTogether(String type) {
        List<IndexManifestEntry> first = centroidModel("first", type);
        IndexManifestFileHandler.GlobalIndexCombiner combiner =
                new IndexManifestFileHandler.GlobalIndexCombiner();
        assertThat(combiner.combine(Collections.emptyList(), first))
                .containsExactlyInAnyOrderElementsOf(first);
        assertThatThrownBy(() -> combiner.combine(Collections.emptyList(), first.subList(0, 2)))
                .hasMessageContaining("Incomplete");
        assertThatThrownBy(
                        () ->
                                combiner.combine(
                                        first,
                                        Collections.singletonList(first.get(1).toDeleteEntry())))
                .hasMessageContaining("Incomplete");
        List<IndexManifestEntry> replacement = centroidModel("second", type);
        assertThatThrownBy(() -> combiner.combine(first, replacement))
                .hasMessageContaining("previous file");
        List<IndexManifestEntry> changes = new ArrayList<>();
        first.forEach(entry -> changes.add(entry.toDeleteEntry()));
        changes.addAll(replacement);
        assertThat(combiner.combine(first, changes))
                .containsExactlyInAnyOrderElementsOf(replacement);
    }

    @Test
    void rejectsMixedIndexTypesAcrossPartitionsAtManifestWrite() {
        IndexFileMeta range =
                new IndexFileMeta(
                        "ivf-pq",
                        "local",
                        10,
                        1,
                        new GlobalIndexMeta(400, 401, 1, null, null),
                        null);
        IndexManifestEntry entry =
                new IndexManifestEntry(FileKind.ADD, BinaryRow.singleColumn(1), 0, range);
        List<IndexManifestEntry> centroid = centroidModel("model", "ivf-pq");
        IndexManifestFile manifest = mock(IndexManifestFile.class);
        IndexManifestFileHandler handler =
                new IndexManifestFileHandler(manifest, BucketMode.BUCKET_UNAWARE);
        when(manifest.read("prior-range")).thenReturn(Collections.singletonList(entry));
        when(manifest.read("prior-centroid")).thenReturn(centroid);
        assertThatThrownBy(() -> handler.write("prior-range", centroid))
                .hasMessageContaining("Drop the previous index");
        assertThatThrownBy(() -> handler.write("prior-centroid", Collections.singletonList(entry)))
                .hasMessageContaining("Drop the previous index");
    }

    private static List<IndexManifestEntry> centroidModel(String id, String type) {
        List<IndexManifestEntry> entries = new ArrayList<>();
        for (int centroid = -1; centroid < 2; centroid++) {
            byte[] metadata =
                    centroid == -1
                            ? VectorIndexShardMeta.routing(
                                            id, 2, Collections.singletonList(new Range(10, 100)))
                                    .serialize()
                            : VectorIndexShardMeta.centroid(id, centroid).serialize();
            IndexFileMeta file =
                    new IndexFileMeta(
                            type + "-centroid",
                            id + "-" + centroid,
                            100,
                            centroid == -1 ? 0 : 10,
                            null,
                            null,
                            new GlobalIndexMeta(10, 100, 1, null, metadata));
            entries.add(new IndexManifestEntry(FileKind.ADD, BinaryRow.EMPTY_ROW, 0, file));
        }
        return entries;
    }
}
