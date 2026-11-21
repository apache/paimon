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

package org.apache.paimon.globalindex;

import org.apache.paimon.fs.Path;
import org.apache.paimon.index.GlobalIndexMeta;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the core-owned layout marker and opaque index metadata. */
class CentroidIndexMetaTest {
    private final Path path = new Path("file:///index");

    @Test
    void rangeFilesPreserveTheirMetadata() {
        for (byte[] payload :
                Arrays.asList(null, new byte[0], new byte[] {0, 2, -1}, new byte[] {'{', '}'})) {
            assertThat(CentroidIndexMeta.isWrapped(payload)).isFalse();
            assertThat(CentroidIndexMeta.file(path, 100, 7, payload).metadata()).isSameAs(payload);
        }
    }

    @Test
    void routedFilesRoundTripOpaqueMetadataAndRoles() {
        for (byte[] payload : Arrays.asList(null, new byte[0], new byte[] {0, 2, -1})) {
            ResultEntry source = new ResultEntry("index", 7, payload);
            ResultEntry shard = CentroidIndexMeta.shard(source, "g", 4);
            ResultEntry router = CentroidIndexMeta.router(source, "g", 5);
            assertThat(shard.meta())
                    .startsWith("PAIMCENT".getBytes(java.nio.charset.StandardCharsets.US_ASCII));
            assertThat(CentroidIndexMeta.isRoutingModel(shard.meta())).isFalse();
            assertThat(CentroidIndexMeta.isRoutingModel(router.meta())).isTrue();
            CentroidIndexFile decoded =
                    (CentroidIndexFile)
                            CentroidIndexMeta.file(path, 100, shard.rowCount(), shard.meta());
            assertThat(decoded.centroid()).isEqualTo(4);
            assertThat(decoded.modelId()).isEqualTo("g");
            assertThat(decoded.rowCount()).isEqualTo(7);
            assertThat(decoded.metadata()).isEqualTo(payload);
            assertThat(
                            ((CentroidIndexFile)
                                            CentroidIndexMeta.file(path, 100, 0, router.meta()))
                                    .nlist())
                    .isEqualTo(5);
        }
    }

    @Test
    void rejectsInvalidWrappedMetadata() {
        byte[] meta =
                CentroidIndexMeta.shard(new ResultEntry("f", 1, new byte[] {42}), "group", 3)
                        .meta();
        assertThatThrownBy(
                        () ->
                                CentroidIndexMeta.file(
                                        path, 10, 1, Arrays.copyOf(meta, meta.length - 1)))
                .hasMessageContaining("length");
        meta[8] = 99;
        assertThatThrownBy(() -> CentroidIndexMeta.file(path, 10, 1, meta))
                .hasMessageContaining("version");
    }

    @Test
    void bitmapRolePairsWithTheVectorGeneration() {
        ResultEntry bitmap =
                CentroidIndexMeta.scalarIndex(new ResultEntry("bitmap", 3, new byte[] {9}), "g", 4);
        CentroidIndexFile decoded =
                (CentroidIndexFile)
                        CentroidIndexMeta.file(path, 100, bitmap.rowCount(), bitmap.meta());
        assertThat(CentroidIndexMeta.hasScalarIndexes(bitmap.meta())).isTrue();
        assertThat(decoded.scalarIndex()).isTrue();
        assertThat(decoded.routingModel()).isFalse();
        assertThat(decoded.centroid()).isEqualTo(4);
        assertThat(decoded.modelId()).isEqualTo("g");
        assertThat(decoded.metadata()).containsExactly(9);
        GlobalIndexMeta meta = new GlobalIndexMeta(0, 9, 1, new int[] {2}, bitmap.meta());
        assertThat(CentroidIndexMeta.matchesIndexFields(meta, Collections.singletonList(1)))
                .isTrue();
        assertThat(CentroidIndexMeta.matchesIndexFields(meta, Collections.singletonList(2)))
                .isFalse();
        GlobalIndexMeta composite = new GlobalIndexMeta(0, 9, 1, new int[] {2}, null);
        assertThat(CentroidIndexMeta.matchesIndexFields(composite, Collections.singletonList(1)))
                .isFalse();
    }
}
