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

import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Source coverage belongs to complete model generations, never to sparse shard bounds. */
class CentroidVectorIndexUtilsTest {
    @Test
    void testSparseShardsDoNotClaimEnvelopeCoverage() {
        List<IndexFileMeta> files = model("a");
        assertThat(CentroidVectorIndexUtils.validateModel(files)).isEqualTo(files.get(0));
        assertThat(CentroidVectorIndexUtils.coverage(files.get(0)))
                .containsExactly(new Range(10, 19), new Range(1000, 1009));
        assertThat(CentroidVectorIndexUtils.coverage(files.get(1))).isEmpty();
        assertThat(CentroidVectorIndexUtils.coverage(files.get(2))).isEmpty();
        assertThat(CentroidVectorIndexUtils.centroidBased(file("{}", false))).isFalse();
    }

    @Test
    void testIncompleteMixedAndMismatchedModelsAreRejected() {
        List<IndexFileMeta> complete = model("a");
        assertThatThrownBy(() -> CentroidVectorIndexUtils.validateModel(complete.subList(0, 2)))
                .hasMessageContaining("Incomplete");
        List<IndexFileMeta> mixed = new ArrayList<>(complete);
        mixed.add(file("{}", false));
        assertThatThrownBy(() -> CentroidVectorIndexUtils.validateModel(mixed))
                .hasMessageContaining("mix");
        List<IndexFileMeta> mismatch = new ArrayList<>(complete);
        mismatch.set(2, model("b").get(2));
        assertThatThrownBy(() -> CentroidVectorIndexUtils.validateModel(mismatch))
                .hasMessageContaining("different models");
        List<IndexFileMeta> duplicate = new ArrayList<>(complete);
        duplicate.add(complete.get(1));
        assertThatThrownBy(() -> CentroidVectorIndexUtils.validateModel(duplicate))
                .hasMessageContaining("Duplicate");
    }

    private static List<IndexFileMeta> model(String id) {
        return Arrays.asList(
                file(
                        VectorIndexShardMeta.routing(
                                        id,
                                        2,
                                        Arrays.asList(new Range(10, 19), new Range(1000, 1009)))
                                .serialize(),
                        true),
                file(VectorIndexShardMeta.centroid(id, 0).serialize(), false),
                file(VectorIndexShardMeta.centroid(id, 1).serialize(), false));
    }

    private static IndexFileMeta file(String json, boolean routing) {
        return file(json.getBytes(java.nio.charset.StandardCharsets.UTF_8), routing);
    }

    private static IndexFileMeta file(byte[] meta, boolean routing) {
        return new IndexFileMeta(
                "ivf-pq",
                Arrays.toString(meta),
                100,
                routing ? 0 : 10,
                null,
                null,
                new GlobalIndexMeta(10, 1009, 7, null, meta),
                routing ? IndexFileMeta.ROUTING_MODEL : IndexFileMeta.DATA_FILE);
    }
}
