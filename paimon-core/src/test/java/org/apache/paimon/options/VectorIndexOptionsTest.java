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
                "pk-vector.index.columns",
                "global-index.ivf-pq.index-column",
                "global-index.ivf-pq.index-columns"
            })
    void testResolutionReturnsMatchingTypeAndOptions(String declaration) {
        Map<String, String> values = new HashMap<>();
        values.put(declaration, "embedding");
        values.put("fields.embedding.pk-vector.index.type", " IVF-PQ ");
        values.put("ivf-pq.dimension", "768");
        String suffix = declaration.startsWith("pk-") ? "pk-vector.index.options" : "index-options";
        values.put("fields.embedding." + suffix, "{\"nlist\":64,\"metric\":\"COSINE\"}");
        Map<String, String> original = new HashMap<>(values);

        VectorIndexOptions resolved =
                VectorIndexOptions.resolve(new Options(values), null, "embedding");
        assertThat(resolved.type()).isEqualTo("ivf-pq");
        assertThat(resolved.options().get("ivf-pq.nlist")).isEqualTo("64");
        assertThat(resolved.options().get("ivf-pq.dimension")).isEqualTo("768");
        assertThat(resolved.metric()).isEqualTo("cosine");
        assertThat(values).isEqualTo(original);

        VectorIndexOptions persisted =
                VectorIndexOptions.resolve(new Options(values), "ivf-flat", "embedding", false);
        assertThat(persisted.type()).isEqualTo("ivf-flat");
        assertThat(persisted.options().get("ivf-flat.nlist")).isEqualTo("64");
        assertThat(persisted.metric()).isEqualTo("cosine");
    }

    @ParameterizedTest
    @ValueSource(strings = {"index-column", "index-columns"})
    void testLegacyRegistrationIsOnlyACompatibilityFallback(String suffix) {
        Options input = new Options();
        input.setString("pk-vector.index.columns", " old, other_old ");
        input.setString("fields.old.pk-vector.index.type", "ivf-pq");
        input.setString("fields.other_old.pk-vector.index.type", "diskann");
        assertThat(VectorIndexOptions.columns(input)).containsExactly("old", "other_old");
        assertThat(VectorIndexOptions.resolve(input, null, "old", false).type())
                .isEqualTo("ivf-pq");
        assertThat(new CoreOptions(input).vectorIndexColumns("ivf-pq")).containsExactly("old");
        assertThat(new CoreOptions(input).vectorIndexColumns("diskann"))
                .containsExactly("other_old");
        assertThat(new CoreOptions(input).vectorIndexColumns("ivf-flat")).isEmpty();
        assertThat(VectorIndexOptions.columnsOptionKey(input)).isEqualTo("pk-vector.index.columns");

        input.setString("global-index.ivf-pq." + suffix, " embedding, other ");
        input.setString("global-index.diskann." + suffix, "video");
        assertThat(VectorIndexOptions.columns(input))
                .containsExactly("video", "embedding", "other");
        assertThat(VectorIndexOptions.resolve(input, null, "embedding", false).type())
                .isEqualTo("ivf-pq");
        assertThat(VectorIndexOptions.resolve(input, null, "video", false).type())
                .isEqualTo("diskann");
        assertThat(new CoreOptions(input).vectorIndexColumns("ivf-pq"))
                .containsExactly("embedding", "other");
        assertThat(new CoreOptions(input).vectorIndexColumns("diskann")).containsExactly("video");
        assertThat(VectorIndexOptions.columnsOptionKey(input))
                .isEqualTo("global-index.diskann." + suffix + ", global-index.ivf-pq." + suffix);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "index_type",
                "index-type",
                "vector.index-type",
                "fields.embedding.index-type"
            })
    void testRemovedTypeAliasesAreIgnored(String key) {
        Options input = Options.fromMap(Collections.singletonMap(key, "ivf-pq"));
        assertThat(VectorIndexOptions.resolve(input, null, "embedding", false).type()).isNull();
        assertThat(VectorIndexOptions.resolve(input, null, "embedding", false).options().toMap())
                .isEqualTo(input.toMap());
        input.setString("fields.embedding.pk-vector.index.type", "diskann");
        assertThat(VectorIndexOptions.resolve(input, null, "embedding", false).type())
                .isEqualTo("diskann");
        input.setString("global-index.ivf-flat.index-columns", "embedding");
        assertThat(VectorIndexOptions.resolve(input, null, "embedding", false).type())
                .isEqualTo("ivf-flat");
    }

    @Test
    void testLegacyPrimaryKeyTypeResolvesFieldParameters() {
        Map<String, String> values = new HashMap<>();
        values.put("fields.embedding.pk-vector.index.type", " IVF-PQ ");
        values.put("ivf-pq.dimension", "768");
        values.put("fields.embedding.index-options", "{\"nlist\":64,\"metric\":\"COSINE\"}");
        Options input = new Options(values);
        assertThat(VectorIndexOptions.resolve(input, null, "embedding", false).type())
                .isEqualTo("ivf-pq");
        Options resolved = VectorIndexOptions.resolve(input, null, "embedding", false).options();
        assertMetric(resolved, "cosine");
        assertThat(resolved.get("ivf-pq.dimension")).isEqualTo("768");
        assertThat(resolved.get("ivf-pq.nlist")).isEqualTo("64");
        assertThat(VectorIndexOptions.resolve(resolved, null, "embedding", false).options().toMap())
                .isEqualTo(resolved.toMap());
        assertThat(input.toMap()).isEqualTo(values);
    }

    @Test
    void testAmbiguousDeclarationsDoNotFallBackToHistoricalHint() {
        Options options = new Options();
        options.setString("global-index.ivf-pq.index-columns", "embedding");
        options.setString("global-index.diskann.index-column", "embedding");
        options.setString("index_type", "ivf-flat");
        assertThat(VectorIndexOptions.resolve(options, null, "embedding", false).type()).isNull();
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
        Options resolved =
                VectorIndexOptions.resolve(new Options(values), null, "embedding").options();
        assertMetric(resolved, expected);
        assertThat(resolved.get("ivf-pq.dimension")).isEqualTo("768");
        assertThat(resolved.get("fields.embedding.nlist")).isEqualTo("48");
        assertThat(VectorIndexOptions.resolve(resolved, "ivf-pq", "embedding").options().toMap())
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
        Options resolved =
                VectorIndexOptions.resolve(new Options(values), null, "embedding").options();
        assertMetric(resolved, "cosine");
        assertThat(resolved.get("ivf-pq.nlist")).isEqualTo("64");
        assertThat(VectorIndexOptions.resolve(resolved, "ivf-pq", "embedding").options().toMap())
                .isEqualTo(resolved.toMap());
        assertThat(
                        VectorIndexOptions.resolve(new Options(values), null, "embedding")
                                .options()
                                .toMap())
                .isEqualTo(resolved.toMap());
    }

    @Test
    void testReaderKeepsUnspecifiedMetricAndWriterUsesDefault() {
        Options input =
                new Options(
                        Collections.singletonMap(
                                "fields.embedding.index-options", "{\"nlist\":64}"));
        Options read = VectorIndexOptions.resolve(input, "ivf-pq", "embedding", false).options();
        assertThat(read.get("ivf-pq.nlist")).isEqualTo("64");
        assertThat(read.get("ivf-pq.metric")).isNull();
        assertThat(read.get("fields.embedding.distance.metric")).isNull();
        assertMetric(
                VectorIndexOptions.resolve(input, "ivf-pq", "embedding").options(),
                "inner_product");
        assertThat(VectorIndexOptions.resolve(input, null, "embedding").options().toMap())
                .isEqualTo(input.toMap());
    }

    private static void assertMetric(Options options, String expected) {
        assertThat(options.get("ivf-pq.metric")).isEqualTo(expected);
        assertThat(options.get("ivf-pq.distance.metric")).isEqualTo(expected);
        assertThat(options.get("fields.embedding.metric")).isEqualTo(expected);
        assertThat(options.get("fields.embedding.distance.metric")).isEqualTo(expected);
    }
}
