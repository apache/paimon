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

package org.apache.paimon.schema;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.schema.SchemaValidation.validateTableSchema;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for primary-key vector index option validation. */
class PrimaryKeyVectorIndexValidationTest {

    @Test
    void testValidPluralPrimaryKeyVectorIndexConfiguration() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "1");
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        options.put(CoreOptions.PK_VECTOR_INDEX_COLUMNS.key(), " embedding ");
        options.put("fields.embedding.pk-vector.index.type", "ivf-pq");

        assertThatCode(() -> validateTableSchema(schema(options))).doesNotThrowAnyException();
    }

    @Test
    void testSupportsMultiplePrimaryKeyVectorIndexColumns() {
        Map<String, String> options = multipleIndexOptions();
        options.put(CoreOptions.PK_INDEX_BUILD_PARALLELISM.key(), "2");
        options.put(CoreOptions.PK_VECTOR_INDEX_COLUMNS.key(), " embedding , other_embedding ");
        options.put("fields.embedding.pk-vector.distance.metric", "cosine");
        options.put("fields.embedding.pk-vector.index.options", "{\"nlist\":64}");
        options.put("fields.other_embedding.pk-vector.distance.metric", "l2");
        options.put("fields.other_embedding.pk-vector.index.options", "{\"nlist\":128}");

        assertThatCode(() -> validateTableSchema(schema(options))).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @CsvSource({"0,true", "-1,true", "0,false", "-1,false"})
    void testRejectsNonPositiveIndexBuildParallelism(int parallelism, boolean vectorIndex) {
        Map<String, String> options = multipleIndexOptions();
        if (!vectorIndex) {
            options.remove(CoreOptions.PK_VECTOR_INDEX_COLUMNS.key());
            options.put(CoreOptions.PK_BTREE_INDEX_COLUMNS.key(), "payload");
        }
        options.put(CoreOptions.PK_INDEX_BUILD_PARALLELISM.key(), String.valueOf(parallelism));

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining("pk-index.build.parallelism must be greater than zero");
    }

    @Test
    void testRequiresIndexTypeForSecondVectorColumn() {
        Map<String, String> options = multipleIndexOptions();
        options.remove("fields.other_embedding.pk-vector.index.type");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining("fields.other_embedding.pk-vector.index.type");
    }

    @Test
    void testRequiresSecondColumnToReferenceVector() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.PK_VECTOR_INDEX_COLUMNS.key(), "embedding,payload");
        options.put("fields.payload.pk-vector.index.type", "ivf-flat");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining("entry 'payload' must reference a VECTOR column");
    }

    @Test
    void testRejectsUnknownSecondVectorColumn() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.PK_VECTOR_INDEX_COLUMNS.key(), "embedding,missing");
        options.put("fields.missing.pk-vector.index.type", "ivf-flat");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining("Field missing can not be found in table schema");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "embedding,"})
    void testRejectsEmptyVectorIndexColumn(String columns) {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.PK_VECTOR_INDEX_COLUMNS.key(), columns);

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining("pk-vector.index.columns")
                .hasMessageContaining("non-empty");
    }

    @Test
    void testRequiresFloatElementsForSecondVectorColumn() {
        List<DataField> fields = new ArrayList<>(fields());
        fields.set(3, new DataField(3, "other_embedding", DataTypes.VECTOR(3, DataTypes.DOUBLE())));

        assertThatThrownBy(() -> validateTableSchema(schema(multipleIndexOptions(), fields)))
                .hasMessageContaining("entry 'other_embedding' must use FLOAT elements");
    }

    @Test
    void testRejectsUnsupportedDistanceMetricForSecondVectorColumn() {
        Map<String, String> options = multipleIndexOptions();
        options.put("fields.other_embedding.pk-vector.distance.metric", "manhattan");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining("fields.other_embedding.pk-vector.distance.metric")
                .hasMessageContaining("l2, cosine, inner_product");
    }

    @Test
    void testRejectsInvalidIndexOptionsForSecondVectorColumn() {
        Map<String, String> options = multipleIndexOptions();
        options.put("fields.other_embedding.pk-vector.index.options", "[1,2]");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining("fields.other_embedding.pk-vector.index.options")
                .hasMessageContaining("JSON object");
    }

    @Test
    void testRejectsSecondVectorColumnInAnotherIndexFamily() {
        Map<String, String> options = multipleIndexOptions();
        options.put(CoreOptions.PK_BTREE_INDEX_COLUMNS.key(), "other_embedding");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining("other_embedding")
                .hasMessageContaining("at most one primary-key index");
    }

    @Test
    void testRejectsDuplicatePrimaryKeyVectorIndexColumns() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "1");
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        options.put(CoreOptions.PK_VECTOR_INDEX_COLUMNS.key(), "embedding, embedding ");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining("pk-vector.index.columns")
                .hasMessageContaining("duplicate");
    }

    @Test
    void testValidPrimaryKeyVectorIndex() {
        assertThatCode(() -> validateTableSchema(schema(enabledOptions())))
                .doesNotThrowAnyException();
    }

    @Test
    void testRequiresFieldScopedIndexType() {
        Map<String, String> options = enabledOptions();
        options.remove("fields.embedding.pk-vector.index.type");
        options.put("pk-vector.index.type", "ivf-pq");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining("fields.embedding.pk-vector.index.type");
    }

    @Test
    void testRequiresPrimaryKeyTable() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.BUCKET_KEY.key(), "id");
        assertThatThrownBy(
                        () ->
                                validateTableSchema(
                                        new TableSchema(
                                                0,
                                                fields(),
                                                0,
                                                Collections.emptyList(),
                                                Collections.emptyList(),
                                                options,
                                                "")))
                .hasMessageContaining("requires a primary-key table");
    }

    @Test
    void testRequiresDeletionVectors() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "false");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining("requires deletion-vectors.enabled = true");
    }

    @Test
    void testSupportsPartialUpdateMergeEngine() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.MERGE_ENGINE.key(), "partial-update");

        assertThatCode(() -> validateTableSchema(schema(options))).doesNotThrowAnyException();
    }

    @Test
    void testSupportsAggregationMergeEngine() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.MERGE_ENGINE.key(), "aggregation");

        assertThatCode(() -> validateTableSchema(schema(options))).doesNotThrowAnyException();
    }

    @Test
    void testIgnoresMergeOnReadForFirstRowWithoutDeletionVectors() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.MERGE_ENGINE.key(), "first-row");
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "false");
        options.put(CoreOptions.DELETION_VECTORS_MERGE_ON_READ.key(), "true");

        assertThatCode(() -> validateTableSchema(schema(options))).doesNotThrowAnyException();
    }

    @Test
    void testRejectsFirstRowWithDeletionVectors() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.MERGE_ENGINE.key(), "first-row");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining(
                        "First row merge engine does not need deletion vectors because there is no deletion of old data in this merge engine");
    }

    @Test
    void testPartialUpdateRejectsDeletionVectorMergeOnRead() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.MERGE_ENGINE.key(), "partial-update");
        options.put(CoreOptions.DELETION_VECTORS_MERGE_ON_READ.key(), "true");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining(
                        "partial-update requires deletion-vectors.merge-on-read = false");
    }

    @Test
    void testDeduplicateRejectsDeletionVectorMergeOnRead() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.DELETION_VECTORS_MERGE_ON_READ.key(), "true");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining("requires deletion-vectors.merge-on-read = false");
    }

    @Test
    void testSupportsPostponeBucket() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.BUCKET.key(), "-2");

        assertThatCode(() -> validateTableSchema(schema(options))).doesNotThrowAnyException();
    }

    @Test
    void testRejectsDynamicBucket() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.BUCKET.key(), "-1");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining("requires fixed or postpone bucket mode");
    }

    @Test
    void testRejectsPkClusteringOverride() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.PK_CLUSTERING_OVERRIDE.key(), "true");
        options.put(CoreOptions.CLUSTERING_COLUMNS.key(), "payload");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining("does not support pk-clustering-override");
    }

    @Test
    void testRequiresVectorColumn() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.PK_VECTOR_INDEX_COLUMNS.key(), "payload");
        options.put("fields.payload.pk-vector.index.type", "ivf-pq");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining("must reference a VECTOR column");
    }

    @Test
    void testIndexNameIsNotRequired() {
        Map<String, String> options = enabledOptions();
        assertThatCode(() -> validateTableSchema(schema(options))).doesNotThrowAnyException();
    }

    @Test
    void testRequiresFloatVectorElements() {
        TableSchema schema =
                new TableSchema(
                        0,
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.INT().notNull()),
                                new DataField(
                                        1, "embedding", DataTypes.VECTOR(8, DataTypes.DOUBLE()))),
                        1,
                        Collections.emptyList(),
                        Collections.singletonList("id"),
                        enabledOptions(),
                        "");

        assertThatThrownBy(() -> validateTableSchema(schema))
                .hasMessageContaining("must use FLOAT elements");
    }

    @Test
    void testRejectsUnsupportedDistanceMetric() {
        Map<String, String> options = enabledOptions();
        options.put("fields.embedding.pk-vector.distance.metric", "manhattan");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining("pk-vector.distance.metric")
                .hasMessageContaining("l2, cosine, inner_product");
    }

    private static Map<String, String> enabledOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "1");
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        options.put(CoreOptions.PK_VECTOR_INDEX_COLUMNS.key(), "embedding");
        options.put("fields.embedding.pk-vector.index.type", "ivf-pq");
        return options;
    }

    private static TableSchema schema(Map<String, String> options) {
        return schema(options, fields());
    }

    private static Map<String, String> multipleIndexOptions() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.PK_VECTOR_INDEX_COLUMNS.key(), "embedding,other_embedding");
        options.put("fields.other_embedding.pk-vector.index.type", "ivf-flat");
        return options;
    }

    private static TableSchema schema(Map<String, String> options, List<DataField> fields) {
        return new TableSchema(
                0,
                fields,
                0,
                Collections.emptyList(),
                Collections.singletonList("id"),
                options,
                "");
    }

    private static List<DataField> fields() {
        return Arrays.asList(
                new DataField(0, "id", DataTypes.INT().notNull()),
                new DataField(1, "embedding", DataTypes.VECTOR(8, DataTypes.FLOAT())),
                new DataField(2, "payload", DataTypes.STRING()),
                new DataField(3, "other_embedding", DataTypes.VECTOR(3, DataTypes.FLOAT())));
    }
}
