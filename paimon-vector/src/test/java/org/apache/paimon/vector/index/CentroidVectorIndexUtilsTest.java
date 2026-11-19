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

package org.apache.paimon.vector.index;

import org.apache.paimon.globalindex.CentroidVectorIndexUtils;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Checks the private metadata of a router and its shards. */
class CentroidVectorIndexUtilsTest {
    @Test
    void indexTypeSelectsTheMetadataCodec() {
        byte[] opaque = {(byte) 0xff, 0, 7, 42};
        VectorIndexShardMeta shard = VectorIndexShardMeta.centroid("g", 7);
        byte[] wrapped = shard.wrap(opaque);
        IndexFileMeta ordinary = file("ivf-pq", wrapped, false);
        assertThat(CentroidVectorIndexUtils.isRoutingModel(ordinary)).isFalse();
        assertThatThrownBy(() -> CentroidVectorIndexUtils.isRoutingModel(file(opaque, false)))
                .hasMessageContaining("Invalid centroid metadata header");
        VectorIndexShardMeta decoded = VectorIndexShardMeta.deserialize(wrapped);
        assertThat(decoded.payloadMeta()).containsExactly(opaque);
        assertThat(decoded.centroid()).isEqualTo(7);
        assertThat(decoded.modelId()).isEqualTo("g");
        assertThat(VectorIndexShardMeta.deserialize(shard.wrap(new byte[0])).payloadMeta())
                .isEmpty();
        assertThat(VectorIndexShardMeta.deserialize(shard.serialize()).payloadMeta()).isNull();
        assertThatThrownBy(
                        () ->
                                VectorIndexShardMeta.deserialize(
                                        Arrays.copyOf(wrapped, wrapped.length - 1)))
                .isInstanceOf(IllegalArgumentException.class);
        // Version 2 shards stored absolute table row IDs.
        wrapped[4] = 2;
        assertThatThrownBy(() -> VectorIndexShardMeta.deserialize(wrapped))
                .hasMessageContaining("version");
    }

    @Test
    void identifiesTheRoutingModelFromPrivateMetadata() {
        assertThat(
                        CentroidVectorIndexUtils.isRoutingModel(
                                file(VectorIndexShardMeta.routing("a", 2, "l2").serialize(), true)))
                .isTrue();
        assertThat(
                        CentroidVectorIndexUtils.isRoutingModel(
                                file(VectorIndexShardMeta.centroid("a", 0).serialize(), false)))
                .isFalse();
    }

    private static IndexFileMeta file(byte[] meta, boolean routing) {
        return file("ivf-pq-centroid", meta, routing);
    }

    private static IndexFileMeta file(String type, byte[] meta, boolean routing) {
        return new IndexFileMeta(
                type,
                Arrays.toString(meta),
                100,
                routing ? 0 : 10,
                null,
                null,
                new GlobalIndexMeta(10, 1009, 7, null, meta));
    }
}
