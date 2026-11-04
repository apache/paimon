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
import org.apache.paimon.options.VectorIndexOptions;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for legacy primary-key vector configuration handled by {@link VectorIndexOptions}. */
class PrimaryKeyVectorIndexOptionsTest {

    @Test
    void testPluralFieldRegistryEnablesIndex() {
        Map<String, String> options = new HashMap<>();
        options.put("pk-vector.index.columns", "embedding");

        assertThat(VectorIndexOptions.columns(new Options(options))).containsExactly("embedding");
    }

    @Test
    void testResolvesSingleIndexColumn() {
        Map<String, String> options = new HashMap<>();
        options.put("pk-vector.index.columns", "embedding");

        assertThat(VectorIndexOptions.columns(new Options(options))).containsExactly("embedding");
    }

    @Test
    void testFieldRegistryIsTheOnlyEnableSwitch() {
        Map<String, String> options = new HashMap<>();
        options.put("pk-vector.index.column", "embedding");
        options.put("pk-vector.index.type", "ivf-pq");

        assertThat(VectorIndexOptions.columns(new Options(options))).isEmpty();
    }

    @Test
    void testIndexTypeMustBeFieldScoped() {
        Map<String, String> options = new HashMap<>();
        options.put("pk-vector.index.columns", "embedding");
        options.put("pk-vector.index.type", "ivf-pq");

        assertThat(
                        VectorIndexOptions.resolve(new Options(options), null, "embedding", false)
                                .type())
                .isNull();
    }

    @Test
    void testIndexOptionsMustBeFieldScoped() {
        Map<String, String> options = new HashMap<>();
        options.put("pk-vector.index.columns", "embedding");
        options.put("fields.embedding.pk-vector.index.type", "ivf-pq");
        options.put("pk-vector.index.options", "{\"nlist\":64}");

        assertThat(
                        VectorIndexOptions.resolve(new Options(options), null, "embedding")
                                .options()
                                .get("ivf-pq.nlist"))
                .isNull();
    }

    @Test
    void testDistanceMetricMustBeFieldScoped() {
        Map<String, String> options = new HashMap<>();
        options.put("pk-vector.index.columns", "embedding");
        options.put("pk-vector.distance.metric", "l2");

        assertThat(VectorIndexOptions.resolve(new Options(options), null, "embedding").metric())
                .isEqualTo("inner_product");
    }

    @Test
    void testFieldScopedDistanceMetricOverridesTableDefault() {
        Map<String, String> options = new HashMap<>();
        options.put("pk-vector.index.columns", "embedding");
        options.put("pk-vector.distance.metric", "l2");
        options.put("fields.embedding.pk-vector.distance.metric", "cosine");

        assertThat(VectorIndexOptions.resolve(new Options(options), null, "embedding").metric())
                .isEqualTo("cosine");
    }

    @Test
    void testFieldScopedJsonOptionsOverrideTableDefault() {
        Map<String, String> options = new HashMap<>();
        options.put("pk-vector.index.columns", "embedding");
        options.put("fields.embedding.pk-vector.index.type", "ivf-pq");
        options.put("pk-vector.index.options", "{\"nlist\":64}");
        options.put("fields.embedding.pk-vector.index.options", "{\"nlist\":128}");

        Options resolved =
                VectorIndexOptions.resolve(new Options(options), null, "embedding").options();

        assertThat(resolved.get("ivf-pq.nlist")).isEqualTo("128");
    }

    @Test
    void testResolvesShortAndQualifiedAlgorithmOptions() {
        CoreOptions coreOptions =
                coreOptions(
                        "{\"nlist\":64,\"ivf-pq.pq.m\":\"8\"," + "\"fields.embedding.hnsw.m\":16}");

        Options resolved =
                VectorIndexOptions.resolve(coreOptions.toConfiguration(), null, "embedding")
                        .options();

        assertThat(resolved.get("ivf-pq.nlist")).isEqualTo("64");
        assertThat(resolved.get("ivf-pq.pq.m")).isEqualTo("8");
        assertThat(resolved.get("fields.embedding.hnsw.m")).isEqualTo("16");
        assertThat(resolved.get("ivf-pq.metric")).isEqualTo("l2");
    }

    @Test
    void testRejectsNonObjectOptions() {
        assertThatThrownBy(
                        () ->
                                VectorIndexOptions.resolve(
                                                coreOptions("[1,2]").toConfiguration(),
                                                null,
                                                "embedding")
                                        .options())
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
        options.put("pk-vector.index.columns", "embedding");
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
