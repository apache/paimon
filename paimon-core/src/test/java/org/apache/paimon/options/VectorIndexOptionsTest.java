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

package org.apache.paimon.options;

import org.apache.paimon.CoreOptions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for vector parameter resolution shared by both table modes. */
class VectorIndexOptionsTest {

    @ParameterizedTest
    @ValueSource(
            strings = {
                "fields.embedding.pk-vector.index.type",
                "index_type",
                "index-type",
                "vector.index-type",
                "fields.embedding.index-type"
            })
    void testLegacyTypeAliasesResolveFieldParameters(String key) {
        Map<String, String> values = new HashMap<>();
        values.put(key, " IVF-PQ ");
        values.put("ivf-pq.dimension", "768");
        values.put("fields.embedding.index-options", "{\"nlist\":64,\"metric\":\"COSINE\"}");
        Options input = new Options(values);
        assertThat(VectorIndexOptions.indexType(input, "embedding")).isEqualTo("ivf-pq");
        assertThat(new CoreOptions(input).vectorIndexType("embedding")).isEqualTo("ivf-pq");
        Options resolved = VectorIndexOptions.resolve(input, null, "embedding", false);
        assertMetric(resolved, "cosine");
        assertThat(resolved.get("ivf-pq.dimension")).isEqualTo("768");
        assertThat(resolved.get("ivf-pq.nlist")).isEqualTo("64");
        assertThat(VectorIndexOptions.resolve(resolved, null, "embedding", false).toMap())
                .isEqualTo(resolved.toMap());
        assertThat(input.toMap()).isEqualTo(values);
    }

    @Test
    void testAmbiguousDeclarationsDoNotFallBackToHistoricalHint() {
        Options options = new Options();
        options.setString("global-index.ivf-pq.index-columns", "embedding");
        options.setString("global-index.diskann.index-column", "embedding");
        options.setString("index_type", "ivf-flat");
        assertThat(VectorIndexOptions.indexType(options, "embedding")).isNull();
    }

    @ParameterizedTest
    @CsvSource({
        "fields.embedding.pk-vector.distance.metric,COSINE,cosine",
        "fields.embedding.distance.metric,COSINE,cosine",
        "fields.embedding.metric,INNER-PRODUCT,inner_product",
        "ivf-pq.distance.metric,COSINE,cosine",
        "ivf-pq.metric,L2,l2",
        "distance.metric,INNER-PRODUCT,inner_product",
        "metric,COSINE,cosine"
    })
    void testLegacyAndDlfMetricAliases(String key, String value, String expected) {
        Map<String, String> values = new HashMap<>();
        values.put("global-index.ivf-pq.index-column", "embedding");
        values.put("ivf-pq.dimension", "768");
        values.put("fields.embedding.nlist", "48");
        values.put(key, value);
        Options resolved = VectorIndexOptions.resolve(new Options(values), null, "embedding");
        assertMetric(resolved, expected);
        assertThat(resolved.get("ivf-pq.dimension")).isEqualTo("768");
        assertThat(resolved.get("fields.embedding.nlist")).isEqualTo("48");
        assertThat(VectorIndexOptions.resolve(resolved, "ivf-pq", "embedding").toMap())
                .isEqualTo(resolved.toMap());
        assertThat(values.get(key)).isEqualTo(value);
    }

    @ParameterizedTest
    @CsvSource({
        "index-options,metric",
        "index-options,distance.metric",
        "pk-vector.index.options,metric"
    })
    void testFieldJsonAliasesRemainIdempotent(String suffix, String key) {
        Map<String, String> values = new HashMap<>();
        values.put("pk-vector.index.columns", "embedding");
        values.put("fields.embedding.pk-vector.index.type", "ivf-pq");
        values.put("fields.embedding.pk-vector.distance.metric", "cosine");
        values.put("fields.embedding." + suffix, "{\"" + key + "\":\"COSINE\",\"nlist\":64}");
        Options resolved = VectorIndexOptions.resolve(new Options(values), null, "embedding");
        assertMetric(resolved, "cosine");
        assertThat(resolved.get("ivf-pq.nlist")).isEqualTo("64");
        assertThat(VectorIndexOptions.resolve(resolved, "ivf-pq", "embedding").toMap())
                .isEqualTo(resolved.toMap());
        assertThat(new CoreOptions(values).primaryKeyVectorIndexOptions("embedding").toMap())
                .isEqualTo(resolved.toMap());
    }

    @Test
    void testReaderKeepsUnspecifiedMetricAndWriterUsesDefault() {
        Options input =
                new Options(
                        Collections.singletonMap(
                                "fields.embedding.index-options", "{\"nlist\":64}"));
        Options read = VectorIndexOptions.resolve(input, "ivf-pq", "embedding", false);
        assertThat(read.get("ivf-pq.nlist")).isEqualTo("64");
        assertThat(read.get("ivf-pq.metric")).isNull();
        assertThat(read.get("fields.embedding.distance.metric")).isNull();
        assertMetric(VectorIndexOptions.resolve(input, "ivf-pq", "embedding"), "inner_product");
        assertThat(VectorIndexOptions.resolve(input, null, "embedding").toMap())
                .isEqualTo(input.toMap());
    }

    private static void assertMetric(Options options, String expected) {
        assertThat(options.get("ivf-pq.metric")).isEqualTo(expected);
        assertThat(options.get("ivf-pq.distance.metric")).isEqualTo(expected);
        assertThat(options.get("fields.embedding.metric")).isEqualTo(expected);
        assertThat(options.get("fields.embedding.distance.metric")).isEqualTo(expected);
    }
}
