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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.schema.SchemaValidation.validateTableSchema;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for primary-key vector index option validation. */
class PrimaryKeyVectorIndexValidationTest {

    @Test
    void testValidPrimaryKeyVectorIndex() {
        assertThatCode(() -> validateTableSchema(schema(enabledOptions())))
                .doesNotThrowAnyException();
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
    void testRequiresFixedBucket() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.BUCKET.key(), "-1");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining("requires fixed bucket mode");
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
        options.put(CoreOptions.PK_VECTOR_INDEX_COLUMN.key(), "payload");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining("must reference a VECTOR column");
    }

    @Test
    void testRequiresCompleteDefinition() {
        Map<String, String> options = enabledOptions();
        options.remove(CoreOptions.PK_VECTOR_INDEX_NAME.key());

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining("pk-vector.index.name must be configured");
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
        options.put(CoreOptions.PK_VECTOR_DISTANCE_METRIC.key(), "manhattan");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining("pk-vector.distance.metric")
                .hasMessageContaining("l2, cosine, inner_product");
    }

    @Test
    void testRejectsInvalidAnnBuildBounds() {
        Map<String, String> invalidRowOptions = enabledOptions();
        invalidRowOptions.put(CoreOptions.PK_VECTOR_ANN_MIN_ROWS.key(), "100");
        invalidRowOptions.put(CoreOptions.PK_VECTOR_ANN_MAX_ROWS.key(), "99");
        assertThatThrownBy(() -> validateTableSchema(schema(invalidRowOptions)))
                .hasMessageContaining("pk-vector.ann.max-rows")
                .hasMessageContaining("greater than or equal");

        Map<String, String> invalidSourceFileOptions = enabledOptions();
        invalidSourceFileOptions.put(CoreOptions.PK_VECTOR_ANN_MAX_SOURCE_FILES.key(), "0");
        assertThatThrownBy(() -> validateTableSchema(schema(invalidSourceFileOptions)))
                .hasMessageContaining("pk-vector.ann.max-source-files")
                .hasMessageContaining("greater than 0");
    }

    private static Map<String, String> enabledOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "1");
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        options.put(CoreOptions.PK_VECTOR_INDEX_NAME.key(), "embedding_index");
        options.put(CoreOptions.PK_VECTOR_INDEX_COLUMN.key(), "embedding");
        options.put(CoreOptions.PK_VECTOR_INDEX_TYPE.key(), "ivf-pq");
        return options;
    }

    private static TableSchema schema(Map<String, String> options) {
        return new TableSchema(
                0,
                fields(),
                0,
                Collections.emptyList(),
                Collections.singletonList("id"),
                options,
                "");
    }

    private static java.util.List<DataField> fields() {
        return Arrays.asList(
                new DataField(0, "id", DataTypes.INT().notNull()),
                new DataField(1, "embedding", DataTypes.VECTOR(8, DataTypes.FLOAT())),
                new DataField(2, "payload", DataTypes.STRING()));
    }
}
