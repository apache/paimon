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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PrimaryKeyVectorIndexOptions}. */
class PrimaryKeyVectorIndexOptionsTest {

    @Test
    void testResolvesShortAndQualifiedAlgorithmOptions() {
        CoreOptions coreOptions =
                coreOptions(
                        "{\"nlist\":64,\"ivf-pq.pq.m\":\"8\"," + "\"fields.embedding.hnsw.m\":16}");

        Options resolved = PrimaryKeyVectorIndexOptions.resolve(coreOptions);

        assertThat(resolved.get("ivf-pq.nlist")).isEqualTo("64");
        assertThat(resolved.get("ivf-pq.pq.m")).isEqualTo("8");
        assertThat(resolved.get("fields.embedding.hnsw.m")).isEqualTo("16");
        assertThat(resolved.get("ivf-pq.metric")).isEqualTo("l2");
    }

    @Test
    void testHashIsCanonicalAcrossJsonPropertyOrder() {
        assertThat(PrimaryKeyVectorIndexOptions.hash(coreOptions("{\"nlist\":64,\"pq.m\":8}")))
                .containsExactly(
                        PrimaryKeyVectorIndexOptions.hash(
                                coreOptions("{\"pq.m\":\"8\",\"nlist\":\"64\"}")));
    }

    @Test
    void testHashIncludesEffectiveTopLevelAlgorithmOptions() {
        assertThat(PrimaryKeyVectorIndexOptions.hash(coreOptions(null, "ivf-pq.nlist", "64")))
                .isNotEqualTo(
                        PrimaryKeyVectorIndexOptions.hash(coreOptions(null, "ivf-pq.nlist", "65")));
    }

    @Test
    void testDefinitionIdIsStableAndDefinitionSensitive() {
        CoreOptions first = coreOptions("{\"nlist\":64,\"pq.m\":8}");
        CoreOptions reordered = coreOptions("{\"pq.m\":8,\"nlist\":64}");
        String definitionId =
                PrimaryKeyVectorIndexOptions.definitionId(7, "VECTOR<FLOAT, 8>", first);

        assertThat(PrimaryKeyVectorIndexOptions.definitionId(7, "VECTOR<FLOAT, 8>", reordered))
                .isEqualTo(definitionId);
        assertThat(PrimaryKeyVectorIndexOptions.definitionId(8, "VECTOR<FLOAT, 8>", first))
                .isNotEqualTo(definitionId);
        assertThat(PrimaryKeyVectorIndexOptions.definitionId(7, "VECTOR<FLOAT, 16>", first))
                .isNotEqualTo(definitionId);
        assertThat(
                        PrimaryKeyVectorIndexOptions.definitionId(
                                7, "VECTOR<FLOAT, 8>", coreOptions("{\"nlist\":65,\"pq.m\":8}")))
                .isNotEqualTo(definitionId);
    }

    @Test
    void testRejectsNonObjectOptions() {
        assertThatThrownBy(() -> PrimaryKeyVectorIndexOptions.resolve(coreOptions("[1,2]")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pk-vector.index.options")
                .hasMessageContaining("JSON object");
    }

    @Test
    void testAnnBuildBoundsDefaults() {
        CoreOptions options = coreOptions(null);

        assertThat(options.primaryKeyVectorAnnMaxRows()).isEqualTo(100_000L);
        assertThat(options.primaryKeyVectorAnnMaxSourceFiles()).isEqualTo(32);
    }

    private static CoreOptions coreOptions(String indexOptions) {
        return coreOptions(indexOptions, null, null);
    }

    private static CoreOptions coreOptions(
            String indexOptions, String additionalKey, String additionalValue) {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.PK_VECTOR_INDEX_TYPE.key(), "ivf-pq");
        options.put(CoreOptions.PK_VECTOR_INDEX_COLUMN.key(), "embedding");
        options.put(CoreOptions.PK_VECTOR_DISTANCE_METRIC.key(), "l2");
        if (indexOptions != null) {
            options.put(CoreOptions.PK_VECTOR_INDEX_OPTIONS.key(), indexOptions);
        }
        if (additionalKey != null) {
            options.put(additionalKey, additionalValue);
        }
        return new CoreOptions(options);
    }
}
