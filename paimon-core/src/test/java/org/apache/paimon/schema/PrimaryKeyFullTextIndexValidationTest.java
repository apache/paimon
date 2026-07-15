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

/** Tests for primary-key full-text index option validation. */
class PrimaryKeyFullTextIndexValidationTest {

    @Test
    void testValidConfiguration() {
        assertThatCode(() -> validateTableSchema(schema(enabledOptions())))
                .doesNotThrowAnyException();
    }

    @Test
    void testRejectsMultipleColumnsForFirstRelease() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.PK_FULL_TEXT_INDEX_COLUMNS.key(), "content,other_content");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining(CoreOptions.PK_FULL_TEXT_INDEX_COLUMNS.key())
                .hasMessageContaining("exactly one column");
    }

    @Test
    void testRejectsDuplicateColumns() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.PK_FULL_TEXT_INDEX_COLUMNS.key(), "content,content");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining(CoreOptions.PK_FULL_TEXT_INDEX_COLUMNS.key())
                .hasMessageContaining("duplicate");
    }

    @Test
    void testRejectsEmptyColumn() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.PK_FULL_TEXT_INDEX_COLUMNS.key(), " ");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining(CoreOptions.PK_FULL_TEXT_INDEX_COLUMNS.key())
                .hasMessageContaining("non-empty column");
    }

    @Test
    void testRejectsUnknownColumn() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.PK_FULL_TEXT_INDEX_COLUMNS.key(), "unknown");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining("entry 'unknown'")
                .hasMessageContaining("existing column");
    }

    @Test
    void testRejectsNonCharacterColumn() {
        TableSchema intContentSchema =
                new TableSchema(
                        0,
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.INT().notNull()),
                                new DataField(1, "content", DataTypes.INT())),
                        0,
                        Collections.emptyList(),
                        Collections.singletonList("id"),
                        enabledOptions(),
                        "");

        assertThatThrownBy(() -> validateTableSchema(intContentSchema))
                .hasMessageContaining("entry 'content'")
                .hasMessageContaining("CHAR/VARCHAR/STRING");
    }

    @Test
    void testRequiresDeletionVectors() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "false");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining("requires deletion-vectors.enabled = true");
    }

    @Test
    void testSupportsFirstRowWithoutDeletionVectors() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.MERGE_ENGINE.key(), "first-row");
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "false");

        assertThatCode(() -> validateTableSchema(schema(options))).doesNotThrowAnyException();
    }

    @Test
    void testRejectsDeletionVectorMergeOnRead() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.DELETION_VECTORS_MERGE_ON_READ.key(), "true");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining("requires deletion-vectors.merge-on-read = false");
    }

    @Test
    void testRejectsDynamicBucket() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.BUCKET.key(), "-1");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining("requires fixed or postpone bucket mode");
    }

    @Test
    void testSupportsPostponeBucket() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.BUCKET.key(), "-2");

        assertThatCode(() -> validateTableSchema(schema(options))).doesNotThrowAnyException();
    }

    @Test
    void testRejectsPkClusteringOverride() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.PK_CLUSTERING_OVERRIDE.key(), "true");
        options.put(CoreOptions.CLUSTERING_COLUMNS.key(), "content");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining("does not support pk-clustering-override");
    }

    @Test
    void testRejectsColumnConfiguredForAnotherPrimaryKeyIndexFamily() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.PK_BTREE_INDEX_COLUMNS.key(), "content");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining("content")
                .hasMessageContaining("at most one primary-key index");
    }

    @Test
    void testRejectsInvalidLsmCompactionOptions() {
        Map<String, String> options = enabledOptions();
        options.put("fields.content.pk-index.compaction.level-fanout", "1");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining("fields.content.pk-index.compaction.level-fanout")
                .hasMessageContaining("greater than 1");
    }

    @Test
    void testRejectsMalformedIndexOptions() {
        Map<String, String> options = enabledOptions();
        options.put("fields.content.pk-full-text.index.options", "{not-json");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining(
                        "fields.content.pk-full-text.index.options must be a JSON object");
    }

    @Test
    void testRejectsConflictingGlobalAndFieldIndexOptions() {
        Map<String, String> options = enabledOptions();
        options.put("full-text.tokenizer", "standard");
        options.put("fields.content.pk-full-text.index.options", "{\"tokenizer\":\"jieba\"}");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining(
                        "fields.content.pk-full-text.index.options defines conflicting values for full-text.tokenizer");
    }

    @Test
    void testRequiresPrimaryKeyTable() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.BUCKET_KEY.key(), "id");

        TableSchema appendTable =
                new TableSchema(
                        0,
                        fields(),
                        0,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        options,
                        "");

        assertThatThrownBy(() -> validateTableSchema(appendTable))
                .hasMessageContaining("Primary-key full-text index requires a primary-key table");
    }

    private static Map<String, String> enabledOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "1");
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        options.put(CoreOptions.PK_FULL_TEXT_INDEX_COLUMNS.key(), "content");
        return options;
    }

    private static java.util.List<DataField> fields() {
        return Arrays.asList(
                new DataField(0, "id", DataTypes.INT().notNull()),
                new DataField(1, "content", DataTypes.STRING()),
                new DataField(2, "other_content", DataTypes.STRING()));
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
}
