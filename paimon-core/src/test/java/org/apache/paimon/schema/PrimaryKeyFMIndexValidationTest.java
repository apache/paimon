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

/** Tests for primary-key FM index option validation. */
class PrimaryKeyFMIndexValidationTest {

    @Test
    void testValidConfiguration() {
        Map<String, String> options = enabledOptions();
        options.put(
                "fields.content.pk-fm.index.options",
                "{\"partition-row-count\":\"2\",\"sa-sample-rate\":\"16\"}");

        assertThatCode(() -> validateTableSchema(schema(options))).doesNotThrowAnyException();
    }

    @Test
    void testRequiresCharacterColumn() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.PK_FM_INDEX_COLUMNS.key(), "id");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining("FM index requires a character string column");
    }

    @Test
    void testRequiresDeletionVectors() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "false");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining("FM indexes require deletion-vectors.enabled = true");
    }

    @Test
    void testRejectsUnknownColumn() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.PK_FM_INDEX_COLUMNS.key(), "unknown");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining(CoreOptions.PK_FM_INDEX_COLUMNS.key())
                .hasMessageContaining("entry 'unknown'");
    }

    @Test
    void testRejectsColumnConfiguredForAnotherFamily() {
        Map<String, String> options = enabledOptions();
        options.put(CoreOptions.PK_BITMAP_INDEX_COLUMNS.key(), "content");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining("content")
                .hasMessageContaining("at most one primary-key index");
    }

    @Test
    void testRejectsMalformedFieldOptions() {
        Map<String, String> options = enabledOptions();
        options.put("fields.content.pk-fm.index.options", "{not-json");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining("fields.content.pk-fm.index.options must be a JSON object");
    }

    @Test
    void testRejectsInvalidFMOptions() {
        Map<String, String> options = enabledOptions();
        options.put("fields.content.pk-fm.index.options", "{\"partition-row-count\":\"0\"}");

        assertThatThrownBy(() -> validateTableSchema(schema(options)))
                .hasMessageContaining("FM index partition row count must be positive");
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
                .hasMessageContaining("FM indexes require a primary-key table");
    }

    private static Map<String, String> enabledOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "1");
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        options.put(CoreOptions.PK_FM_INDEX_COLUMNS.key(), "content");
        return options;
    }

    private static java.util.List<DataField> fields() {
        return Arrays.asList(
                new DataField(0, "id", DataTypes.INT().notNull()),
                new DataField(1, "content", DataTypes.STRING()));
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
