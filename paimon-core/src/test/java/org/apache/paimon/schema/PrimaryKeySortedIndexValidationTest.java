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

/** Tests for primary-key BTree and Bitmap index validation. */
class PrimaryKeySortedIndexValidationTest {

    @Test
    void testRejectsColumnConfiguredForMultipleIndexFamilies() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.PK_BTREE_INDEX_COLUMNS.key(), "payload");
        options.put(CoreOptions.PK_BITMAP_INDEX_COLUMNS.key(), "payload");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining("payload")
                .hasMessageContaining("at most one primary-key index");
    }

    @Test
    void testRejectsDuplicateBTreeIndexColumns() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.PK_BTREE_INDEX_COLUMNS.key(), "payload,payload");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining(CoreOptions.PK_BTREE_INDEX_COLUMNS.key())
                .hasMessageContaining("duplicate");
    }

    @Test
    void testRejectsDuplicateBitmapIndexColumns() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.PK_BITMAP_INDEX_COLUMNS.key(), "payload,payload");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining(CoreOptions.PK_BITMAP_INDEX_COLUMNS.key())
                .hasMessageContaining("duplicate");
    }

    @Test
    void testRequiresDeletionVectors() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "false");
        options.put(CoreOptions.PK_BTREE_INDEX_COLUMNS.key(), "payload");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining(
                        "Primary-key BTree and Bitmap indexes require deletion-vectors.enabled = true");
    }

    @Test
    void testRequiresPrimaryKeyTable() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.BUCKET_KEY.key(), "id");
        options.put(CoreOptions.PK_BITMAP_INDEX_COLUMNS.key(), "payload");

        assertThatThrownBy(() -> validateTableSchema(schema(options, Collections.emptyList())))
                .hasMessageContaining(
                        "Primary-key BTree and Bitmap indexes require a primary-key table");
    }

    @Test
    void testRequiresFixedBucketMode() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.BUCKET.key(), "-1");
        options.put(CoreOptions.PK_BTREE_INDEX_COLUMNS.key(), "payload");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining(
                        "Primary-key BTree and Bitmap indexes require fixed or postpone bucket mode");
    }

    @Test
    void testSupportsPostponeBucket() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.BUCKET.key(), "-2");
        options.put(CoreOptions.PK_BTREE_INDEX_COLUMNS.key(), "payload");

        assertThatCode(() -> validateTableSchema(schema(options))).doesNotThrowAnyException();
    }

    @Test
    void testRejectsDeletionVectorMergeOnRead() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.DELETION_VECTORS_MERGE_ON_READ.key(), "true");
        options.put(CoreOptions.PK_BITMAP_INDEX_COLUMNS.key(), "payload");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining(
                        "Primary-key BTree and Bitmap indexes require deletion-vectors.merge-on-read = false");
    }

    @Test
    void testRejectsPkClusteringOverride() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.PK_CLUSTERING_OVERRIDE.key(), "true");
        options.put(CoreOptions.CLUSTERING_COLUMNS.key(), "payload");
        options.put(CoreOptions.PK_BTREE_INDEX_COLUMNS.key(), "payload");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining(
                        "Primary-key BTree and Bitmap indexes do not support pk-clustering-override");
    }

    @Test
    void testRejectsUnknownIndexColumn() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.PK_BITMAP_INDEX_COLUMNS.key(), "unknown");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining("pk-bitmap.index.columns entry 'unknown'")
                .hasMessageContaining("must reference an existing column");
    }

    @Test
    void testRejectsUnsupportedIndexColumnType() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.PK_BTREE_INDEX_COLUMNS.key(), "payload");

        TableSchema schema =
                new TableSchema(
                        0,
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.INT().notNull()),
                                new DataField(1, "payload", DataTypes.ARRAY(DataTypes.INT()))),
                        0,
                        Collections.emptyList(),
                        Collections.singletonList("id"),
                        options,
                        "");

        assertThatThrownBy(() -> validateTableSchema(schema))
                .hasMessageContaining("ARRAY<INT>")
                .hasMessageContaining("not supported by global index");
    }

    private static Map<String, String> enabledOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "1");
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        return options;
    }

    private static TableSchema schema(Map<String, String> options) {
        return schema(options, Collections.singletonList("id"));
    }

    private static TableSchema schema(
            Map<String, String> options, java.util.List<String> primaryKeys) {
        return new TableSchema(
                0,
                Arrays.asList(
                        new DataField(0, "id", DataTypes.INT().notNull()),
                        new DataField(1, "payload", DataTypes.STRING())),
                0,
                Collections.emptyList(),
                primaryKeys,
                options,
                "");
    }
}
