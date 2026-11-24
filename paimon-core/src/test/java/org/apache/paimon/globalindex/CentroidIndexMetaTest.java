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

import org.apache.paimon.shade.guava30.com.google.common.io.BaseEncoding;

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
    void writesTheSingleVersionWithFixedWidthFields() {
        ResultEntry shard =
                CentroidIndexMeta.shard(new ResultEntry("shard", 7, new byte[] {42}), 4);
        assertThat(shard.meta())
                .containsExactly(
                        BaseEncoding.base16()
                                .lowerCase()
                                .decode(
                                        "5041494d43454e54" // PAIMCENT
                                                + "0153" // Version and shard role
                                                + "00000004" // Centroid ID
                                                + "2a")); // Payload
    }

    @Test
    void routedFilesRoundTripOpaqueMetadataAndRoles() {
        for (byte[] payload : Arrays.asList(null, new byte[] {0, 2, -1})) {
            ResultEntry source = new ResultEntry("index", 7, payload);
            ResultEntry shard = CentroidIndexMeta.shard(source, 4);
            ResultEntry router = CentroidIndexMeta.router(source, 5);
            assertThat(CentroidIndexMeta.isRoutingModel(shard.meta())).isFalse();
            assertThat(CentroidIndexMeta.isRoutingModel(router.meta())).isTrue();
            CentroidIndexFile decoded =
                    (CentroidIndexFile)
                            CentroidIndexMeta.file(path, 100, shard.rowCount(), shard.meta());
            assertThat(decoded.centroid()).isEqualTo(4);
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
        byte[] meta = CentroidIndexMeta.shard(new ResultEntry("f", 1, new byte[] {42}), 3).meta();
        assertThatThrownBy(() -> CentroidIndexMeta.file(path, 10, 1, Arrays.copyOf(meta, 13)))
                .hasMessageContaining("Truncated");
        meta[8] = 2;
        assertThatThrownBy(() -> CentroidIndexMeta.file(path, 10, 1, meta))
                .hasMessageContaining("version");
        meta[8] = 1;
        meta[9] = 2;
        assertThatThrownBy(() -> CentroidIndexMeta.file(path, 10, 1, meta))
                .hasMessageContaining("role");
    }

    @Test
    void bitmapRoleAndExtraFieldsDescribeTheIndex() {
        ResultEntry bitmap =
                CentroidIndexMeta.scalarIndex(new ResultEntry("bitmap", 3, new byte[] {9}), 4);
        CentroidIndexFile decoded =
                (CentroidIndexFile)
                        CentroidIndexMeta.file(path, 100, bitmap.rowCount(), bitmap.meta());
        assertThat(decoded.scalarIndex()).isTrue();
        assertThat(decoded.routingModel()).isFalse();
        assertThat(decoded.centroid()).isEqualTo(4);
        assertThat(decoded.metadata()).containsExactly(9);
        assertThat(bitmap.meta()[8]).isEqualTo((byte) 1);

        ResultEntry emptyRouter = CentroidIndexMeta.router(new ResultEntry("router", 0, null), 0);
        GlobalIndexMeta meta = new GlobalIndexMeta(0, 9, 1, new int[] {2}, emptyRouter.meta());
        assertThat(CentroidIndexMeta.hasBitmapFields(meta)).isTrue();
        assertThat(
                        CentroidIndexMeta.hasBitmapFields(
                                new GlobalIndexMeta(0, 9, 1, null, emptyRouter.meta())))
                .isFalse();
        assertThat(CentroidIndexMeta.matchesIndexFields(meta, Collections.singletonList(1)))
                .isTrue();
        assertThat(CentroidIndexMeta.matchesIndexFields(meta, Collections.singletonList(2)))
                .isFalse();
        GlobalIndexMeta composite = new GlobalIndexMeta(0, 9, 1, new int[] {2}, null);
        assertThat(CentroidIndexMeta.matchesIndexFields(composite, Collections.singletonList(1)))
                .isFalse();
    }
}
