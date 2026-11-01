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

package org.apache.paimon;

import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.SortedGlobalIndexer;
import org.apache.paimon.globalindex.VectorGlobalIndexer;
import org.apache.paimon.index.pk.PrimaryKeyIndexDefinition;
import org.apache.paimon.index.pk.PrimaryKeyIndexDefinitions;
import org.apache.paimon.options.Options;
import org.apache.paimon.options.VectorIndexOptions;
import org.apache.paimon.schema.SchemaValidation;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Configuration contracts shared by primary-key and Data Evolution indexes. */
class GlobalIndexOptionsTest {

    @ParameterizedTest
    @ValueSource(
            strings = {
                "index_type",
                "index-type",
                "vector.index-type",
                "fields.embedding.index-type"
            })
    void testGenericMergeDoesNotInferHistoricalVectorTypes(String key) {
        Map<String, String> values = new HashMap<>();
        values.put(key, "ivf-pq");
        values.put("fields.embedding.index-options", "{\"nlist\":64}");
        assertThat(new CoreOptions(values).globalIndexOptions(null, "embedding").toMap())
                .isEqualTo(values);
    }

    @Test
    void testGenericMergeDoesNotInterpretVectorParameters() {
        Map<String, String> values = new HashMap<>();
        values.put("fields.embedding.index-options", "{\"metric\":\"INNER-PRODUCT\",\"nlist\":64}");
        Options resolved = new CoreOptions(values).globalIndexOptions("ivf-pq", "embedding");
        assertThat(resolved.get("ivf-pq.metric")).isEqualTo("INNER-PRODUCT");
        assertThat(resolved.get("ivf-pq.distance.metric")).isNull();
        assertThat(
                        new CoreOptions(Collections.emptyMap())
                                .globalIndexOptions("ivf-pq", "embedding")
                                .get("ivf-pq.metric"))
                .isNull();
    }

    @ParameterizedTest
    @CsvSource({
        "btree,btree",
        "bitmap,bitmap",
        "multivalue,multivalue",
        "fm,fm",
        "full-text,full-text",
        "ivf-pq,vector"
    })
    void testColumnDeclarationsAndLegacyFallback(String type, String legacyFamily) {
        Map<String, String> values = new HashMap<>();
        values.put("pk-" + legacyFamily + ".index.columns", " old_column ");
        values.put("fields.old_column.pk-vector.index.type", "ivf-pq");
        CoreOptions options = new CoreOptions(values);
        assertThat(
                        CoreOptions.isVectorIndexType(type)
                                ? VectorIndexOptions.columns(options.toConfiguration())
                                : options.globalIndexColumns(type))
                .containsExactly("old_column");

        values.put("global-index." + type + ".index-column", "singular_column");
        options = new CoreOptions(values);
        assertThat(options.globalIndexColumns(type)).containsExactly("singular_column");
        assertThat(options.globalIndexTypes()).containsExactly(type);

        values.put("global-index." + type + ".index-columns", " first, second ");
        options = new CoreOptions(values);
        assertThat(options.globalIndexColumns(type)).containsExactly("first", "second");
        if ("ivf-pq".equals(type)) {
            assertThat(VectorIndexOptions.columns(options.toConfiguration()))
                    .containsExactly("first", "second");
            assertThat(
                            VectorIndexOptions.resolve(
                                            options.toConfiguration(), null, "first", false)
                                    .type())
                    .isEqualTo("ivf-pq");
        }
    }

    @Test
    void testSingularColumnOptionIsRecognized() {
        CoreOptions options =
                new CoreOptions(
                        Collections.singletonMap("global-index.btree.index-column", "amount"));
        assertThat(options.globalIndexTypes()).containsExactly("btree");
        assertThat(options.btreeIndexColumns()).containsExactly("amount");
    }

    @ParameterizedTest
    @CsvSource({
        "btree,btree-index.,block-size,4 kb,8 kb",
        "bitmap,bitmap-index.,dictionary-block-size,4 kb,8 kb",
        "multivalue,multivalue-index.,dictionary-block-size,4 kb,8 kb",
        "fm,fm-index.,sa-sample-rate,16,32",
        "full-text,full-text.,tokenizer,default,jieba",
        "ivf-pq,ivf-pq.,nlist,32,64"
    })
    void testFieldOptionsOverrideTypeDefaults(
            String type, String prefix, String key, String tableValue, String fieldValue) {
        Map<String, String> values = new HashMap<>();
        values.put(prefix + key, tableValue);
        values.put("fields.payload.index-options", "{\"" + key + "\":\"" + fieldValue + "\"}");
        values.put(
                "fields.payload.pk-" + ("ivf-pq".equals(type) ? "vector" : type) + ".index.options",
                "invalid legacy JSON");
        values.put("sorted-index.records-per-file", "20");
        Options options = new CoreOptions(values).globalIndexOptions(type, "payload");
        assertThat(options.get(prefix + key)).isEqualTo(fieldValue);
        assertThat(options.get("sorted-index.records-per-file")).isEqualTo("20");
        assertThat(values.get(prefix + key)).isEqualTo(tableValue);
        assertThat(new CoreOptions(options).globalIndexOptions(type, "payload").toMap())
                .isEqualTo(options.toMap());
    }

    @ParameterizedTest
    @ValueSource(strings = {"COSINE", "inner-product", "L2"})
    void testLegacyPrimaryKeyMetricSurvivesFactoryResolution(String metric) {
        Map<String, String> values = new HashMap<>();
        values.put("pk-vector.index.columns", "embedding");
        values.put("fields.embedding.pk-vector.index.type", "test-vector-ann");
        values.put(
                "fields.embedding.pk-vector.distance.metric",
                metric.toLowerCase().replace('-', '_'));
        values.put("fields.embedding.pk-vector.index.options", "{\"metric\":\"" + metric + "\"}");
        TableSchema schema = schema(values, true);
        PrimaryKeyIndexDefinition definition =
                PrimaryKeyIndexDefinitions.create(schema).definitions().get(0);

        assertThat(
                        VectorIndexOptions.resolve(
                                        definition.options(), definition.indexType(), "embedding")
                                .options()
                                .toMap())
                .isEqualTo(definition.options().toMap());
        assertThat(
                        GlobalIndexer.create(
                                definition.indexType(),
                                Collections.singletonList(schema.nameToFieldMap().get("embedding")),
                                definition.options()))
                .isInstanceOf(VectorGlobalIndexer.class);
        assertThat(values.get("fields.embedding.pk-vector.index.options"))
                .isEqualTo("{\"metric\":\"" + metric + "\"}");
    }

    @Test
    void testCompositeIndexResolvesOptionsForLeadingField() {
        Map<String, String> values = new HashMap<>();
        values.put("btree-index.fallback-scan-max-size", "invalid table default");
        values.put("fields.amount.index-options", "{\"fallback-scan-max-size\":\"1 mb\"}");
        values.put("fields.id.index-options", "invalid options for a payload field");
        TableSchema schema = schema(values, false);

        assertThat(
                        GlobalIndexer.create(
                                "btree",
                                Arrays.asList(
                                        schema.nameToFieldMap().get("amount"),
                                        schema.nameToFieldMap().get("id")),
                                new Options(values)))
                .isInstanceOf(SortedGlobalIndexer.class);
    }

    @Test
    void testTypedFullTextFieldOptionsUseNativeEncoding() {
        Options resolved =
                new CoreOptions(
                                Collections.singletonMap(
                                        "fields.text.index-options",
                                        "{\"lower-case\":false,\"stop-words\":[\"paimon\",\"lake\"]}"))
                        .globalIndexOptions("full-text", "text");
        assertThat(resolved.get("full-text.lower-case")).isEqualTo("false");
        assertThat(resolved.get("full-text.stop-words")).isEqualTo("paimon;lake");
    }

    @Test
    void testUnrelatedFieldOptionsDoNotRequestDefaultMetric() {
        CoreOptions options =
                new CoreOptions(
                        Collections.singletonMap(
                                "fields.embedding.index-options", "{\"nlist\":64}"));
        assertThat(
                        VectorIndexOptions.resolve(
                                        options.toConfiguration(), "ivf-pq", "embedding", false)
                                .options()
                                .get("ivf-pq.metric"))
                .isNull();
    }

    @Test
    void testDocumentedVectorOptionsAndFieldOverrides() {
        Map<String, String> values = new HashMap<>();
        values.put("global-index.ivf-pq.index-columns", "embedding");
        values.put("ivf-pq.dimension", "768");
        values.put("ivf-pq.distance.metric", "l2");
        values.put("ivf-pq.nlist", "32");
        values.put("fields.embedding.nlist", "48");
        values.put("fields.embedding.distance.metric", "cosine");
        CoreOptions options = new CoreOptions(values);
        assertThat(
                        VectorIndexOptions.resolve(options.toConfiguration(), null, "embedding")
                                .metric())
                .isEqualTo("cosine");
        assertThat(
                        VectorIndexOptions.resolve(options.toConfiguration(), null, "embedding")
                                .options()
                                .get("ivf-pq.dimension"))
                .isEqualTo("768");

        values.put(
                "fields.embedding.index-options",
                "{\"nlist\":64,\"distance.metric\":\"inner_product\"}");
        options = new CoreOptions(values);
        assertThat(
                        VectorIndexOptions.resolve(options.toConfiguration(), null, "embedding")
                                .metric())
                .isEqualTo("inner_product");
        Options resolved =
                VectorIndexOptions.resolve(options.toConfiguration(), null, "embedding").options();
        assertThat(resolved.get("ivf-pq.nlist")).isEqualTo("64");
        assertThat(resolved.get("fields.embedding.nlist")).isEqualTo("64");
        assertThat(resolved.get("ivf-pq.metric")).isEqualTo("inner_product");
        assertThat(resolved.get("ivf-pq.distance.metric")).isEqualTo("inner_product");
        assertThat(resolved.get("fields.embedding.distance.metric")).isEqualTo("inner_product");
    }

    @ParameterizedTest
    @ValueSource(strings = {"[]", "{\"nlist\":null}", "{\"\":64}", "invalid"})
    void testRejectsMalformedSharedFieldOptions(String json) {
        CoreOptions options =
                new CoreOptions(Collections.singletonMap("fields.embedding.index-options", json));
        assertThatThrownBy(() -> options.globalIndexOptions("ivf-pq", "embedding"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fields.embedding.index-options");
    }

    @Test
    void testPrimaryKeyAndDataEvolutionValidationUseSameDeclarations() {
        Map<String, String> values = new HashMap<>();
        values.put("global-index.btree.index-columns", "amount");
        values.put("global-index.ivf-pq.index-columns", "embedding");
        values.put("fields.embedding.index-options", "{\"nlist\":64}");
        values.put("bucket", "1");
        values.put("deletion-vectors.enabled", "true");
        assertThatCode(() -> SchemaValidation.validateTableSchema(schema(values, true)))
                .doesNotThrowAnyException();

        values.put("bucket", "-1");
        values.put("row-tracking.enabled", "true");
        values.put("data-evolution.enabled", "true");
        values.remove("deletion-vectors.enabled");
        values.put("global-index.diskann.index-columns", "embedding");
        assertThatCode(() -> SchemaValidation.validateTableSchema(schema(values, false)))
                .doesNotThrowAnyException();
    }

    @ParameterizedTest
    @CsvSource({
        "btree,btree",
        "bitmap,bitmap",
        "multivalue,multivalue",
        "fm,fm",
        "full-text,full-text",
        "ivf-pq,vector"
    })
    void testIndexOptionsAreNotValidatedWithSchema(String type, String legacyFamily) {
        Map<String, String> values = new HashMap<>();
        values.put("global-index." + type + ".index-columns", "unknown,,amount,amount");
        values.put("pk-" + legacyFamily + ".index.columns", "unknown,,amount,amount");
        values.put("fields.amount.index-options", "invalid JSON");
        values.put("fields.amount.pk-" + legacyFamily + ".index.options", "invalid JSON");
        values.put("bucket", "1");
        assertThatCode(() -> SchemaValidation.validateTableSchema(schema(values, true)))
                .doesNotThrowAnyException();

        values.put("bucket", "-1");
        values.put("row-tracking.enabled", "true");
        values.put("data-evolution.enabled", "true");
        assertThatCode(() -> SchemaValidation.validateTableSchema(schema(values, false)))
                .doesNotThrowAnyException();
    }

    @ParameterizedTest
    @ValueSource(strings = {"amount,amount", "unknown", "amount,", ""})
    void testSchemaDoesNotValidateIndexColumnDeclarations(String columns) {
        Map<String, String> values = new HashMap<>();
        values.put("global-index.btree.index-columns", columns);
        values.put("bucket", "-1");
        values.put("row-tracking.enabled", "true");
        values.put("data-evolution.enabled", "true");
        assertThatCode(() -> SchemaValidation.validateTableSchema(schema(values, false)))
                .doesNotThrowAnyException();
    }

    private static TableSchema schema(Map<String, String> options, boolean primaryKey) {
        return new TableSchema(
                0,
                Arrays.asList(
                        new DataField(0, "id", DataTypes.INT().notNull()),
                        new DataField(1, "amount", DataTypes.INT()),
                        new DataField(2, "embedding", DataTypes.VECTOR(768, DataTypes.FLOAT()))),
                2,
                Collections.emptyList(),
                primaryKey ? Collections.singletonList("id") : Collections.emptyList(),
                options,
                "");
    }
}
