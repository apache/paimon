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
    void testPluralFieldRegistryEnablesIndex() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.PK_VECTOR_INDEX_COLUMNS.key(), "embedding");

        assertThat(new CoreOptions(options).primaryKeyVectorIndexEnabled()).isTrue();
    }

    @Test
    void testFieldRegistryIsTheOnlyEnableSwitch() {
        Map<String, String> options = new HashMap<>();
        options.put("pk-vector.index.column", "embedding");
        options.put("pk-vector.index.type", "ivf-pq");

        assertThat(new CoreOptions(options).primaryKeyVectorIndexEnabled()).isFalse();
    }

    @Test
    void testIndexTypeMustBeFieldScoped() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.PK_VECTOR_INDEX_COLUMNS.key(), "embedding");
        options.put("pk-vector.index.type", "ivf-pq");

        assertThat(new CoreOptions(options).primaryKeyVectorIndexType("embedding")).isNull();
    }

    @Test
    void testIndexOptionsMustBeFieldScoped() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.PK_VECTOR_INDEX_COLUMNS.key(), "embedding");
        options.put("pk-vector.index.options", "{\"nlist\":64}");

        assertThat(new CoreOptions(options).primaryKeyVectorIndexOptions("embedding")).isNull();
    }

    @Test
    void testDistanceMetricMustBeFieldScoped() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.PK_VECTOR_INDEX_COLUMNS.key(), "embedding");
        options.put("pk-vector.distance.metric", "l2");

        assertThat(new CoreOptions(options).primaryKeyVectorDistanceMetric("embedding"))
                .isEqualTo("inner_product");
    }

    @Test
    void testFieldScopedDistanceMetricOverridesTableDefault() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.PK_VECTOR_INDEX_COLUMNS.key(), "embedding");
        options.put("pk-vector.distance.metric", "l2");
        options.put("fields.embedding.pk-vector.distance.metric", "cosine");

        assertThat(new CoreOptions(options).primaryKeyVectorDistanceMetric("embedding"))
                .isEqualTo("cosine");
    }

    @Test
    void testFieldScopedAnnThresholdOverridesTableDefault() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.PK_VECTOR_INDEX_COLUMNS.key(), "embedding");
        options.put(CoreOptions.PK_VECTOR_ANN_MIN_ROWS.key(), "10000");
        options.put("fields.embedding.pk-vector.ann.min-rows", "20000");

        CoreOptions coreOptions = new CoreOptions(options);
        assertThat(coreOptions.primaryKeyVectorAnnMinRows("embedding")).isEqualTo(20_000L);
    }

    @Test
    void testFieldScopedJsonOptionsOverrideTableDefault() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.PK_VECTOR_INDEX_COLUMNS.key(), "embedding");
        options.put("fields.embedding.pk-vector.index.type", "ivf-pq");
        options.put("pk-vector.index.options", "{\"nlist\":64}");
        options.put("fields.embedding.pk-vector.index.options", "{\"nlist\":128}");

        Options resolved = PrimaryKeyVectorIndexOptions.resolve(new CoreOptions(options));

        assertThat(resolved.get("ivf-pq.nlist")).isEqualTo("128");
    }

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
    void testRejectsNonObjectOptions() {
        assertThatThrownBy(() -> PrimaryKeyVectorIndexOptions.resolve(coreOptions("[1,2]")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pk-vector.index.options")
                .hasMessageContaining("JSON object");
    }

    private static CoreOptions coreOptions(String indexOptions) {
        return coreOptions(indexOptions, null, null);
    }

    private static CoreOptions coreOptions(
            String indexOptions, String additionalKey, String additionalValue) {
        Map<String, String> options = new HashMap<>();
        options.put("fields.embedding.pk-vector.index.type", "ivf-pq");
        options.put(CoreOptions.PK_VECTOR_INDEX_COLUMNS.key(), "embedding");
        options.put("fields.embedding.pk-vector.distance.metric", "l2");
        if (indexOptions != null) {
            options.put("fields.embedding.pk-vector.index.options", indexOptions);
        }
        if (additionalKey != null) {
            options.put(additionalKey, additionalValue);
        }
        return new CoreOptions(options);
    }
}
