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

package org.apache.paimon.index.pk;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PrimaryKeyIndexDefinitions}. */
class PrimaryKeyIndexDefinitionsTest {

    @Test
    void testCreatesMixedDefinitionsInSchemaFieldOrder() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.PK_VECTOR_INDEX_COLUMNS.key(), "embedding");
        options.put("fields.embedding.pk-vector.index.type", "ivf-pq");
        options.put(CoreOptions.PK_BTREE_INDEX_COLUMNS.key(), "name");
        options.put(CoreOptions.PK_BITMAP_INDEX_COLUMNS.key(), "status");

        List<PrimaryKeyIndexDefinition> definitions =
                PrimaryKeyIndexDefinitions.create(schema(options)).definitions();

        assertThat(definitions)
                .extracting(PrimaryKeyIndexDefinition::column)
                .containsExactly("name", "status", "embedding");
        assertThat(definitions)
                .extracting(PrimaryKeyIndexDefinition::family)
                .containsExactly(
                        PrimaryKeyIndexDefinition.Family.BTREE,
                        PrimaryKeyIndexDefinition.Family.BITMAP,
                        PrimaryKeyIndexDefinition.Family.VECTOR);
        assertThat(definitions)
                .extracting(PrimaryKeyIndexDefinition::fieldId)
                .containsExactly(1, 2, 3);
        assertThat(definitions)
                .extracting(PrimaryKeyIndexDefinition::indexType)
                .containsExactly("btree", "bitmap", "ivf-pq");
        assertThat(definitions)
                .extracting(PrimaryKeyIndexDefinition::compactionLevelFanout)
                .containsOnly(5);
        assertThat(definitions)
                .extracting(PrimaryKeyIndexDefinition::compactionStaleRatioThreshold)
                .containsOnly(0.2);
    }

    @Test
    void testResolvesFieldScopedCompactionOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.PK_BTREE_INDEX_COLUMNS.key(), "name");
        options.put("fields.name.pk-index.compaction.level-fanout", "7");
        options.put("fields.name.pk-index.compaction.stale-ratio-threshold", "0.4");

        PrimaryKeyIndexDefinition definition =
                PrimaryKeyIndexDefinitions.create(schema(options)).definitions().get(0);

        assertThat(definition.compactionLevelFanout()).isEqualTo(7);
        assertThat(definition.compactionStaleRatioThreshold()).isEqualTo(0.4);
    }

    @Test
    void testCreatesFullTextDefinitionWithoutIndexType() {
        Map<String, String> options = new HashMap<>();
        options.put("pk-full-text.index.columns", "name");

        List<PrimaryKeyIndexDefinition> definitions =
                PrimaryKeyIndexDefinitions.create(schema(options)).definitions();

        assertThat(definitions).hasSize(1);
        assertThat(definitions.get(0).column()).isEqualTo("name");
        assertThat(definitions.get(0).indexType()).isEqualTo("full-text");
        assertThat(definitions.get(0).family().name()).isEqualTo("FULL_TEXT");
    }

    @Test
    void testResolvesFullTextIndexOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("pk-full-text.index.columns", "name");
        options.put(
                "fields.name.pk-full-text.index.options",
                "{\"full-text.tokenizer\":\"jieba\",\"ngram.min-gram\":\"2\"}");

        PrimaryKeyIndexDefinition definition =
                PrimaryKeyIndexDefinitions.create(schema(options)).definitions().get(0);

        assertThat(definition.options().get("full-text.tokenizer")).isEqualTo("jieba");
        assertThat(definition.options().get("full-text.ngram.min-gram")).isEqualTo("2");
        assertThat(definition.options().toMap())
                .doesNotContainKey("pk-full-text.index.columns")
                .doesNotContainKey("fields.name.pk-full-text.index.options");
    }

    @Test
    void testFullTextDefinitionFingerprintIsStableAndOptionSensitive() {
        Map<String, String> left = new HashMap<>();
        left.put("pk-full-text.index.columns", "name");
        left.put(
                "fields.name.pk-full-text.index.options",
                "{\"full-text.tokenizer\":\"ngram\",\"ngram.min-gram\":\"2\"}");
        Map<String, String> reordered = new HashMap<>();
        reordered.put("pk-full-text.index.columns", "name");
        reordered.put(
                "fields.name.pk-full-text.index.options",
                "{\"ngram.min-gram\":\"2\",\"full-text.tokenizer\":\"ngram\"}");
        Map<String, String> changed = new HashMap<>(left);
        changed.put(
                "fields.name.pk-full-text.index.options",
                "{\"full-text.tokenizer\":\"ngram\",\"ngram.min-gram\":\"3\"}");

        String fingerprint =
                PrimaryKeyIndexDefinitions.create(schema(left))
                        .definitions()
                        .get(0)
                        .definitionFingerprint();
        String reorderedFingerprint =
                PrimaryKeyIndexDefinitions.create(schema(reordered))
                        .definitions()
                        .get(0)
                        .definitionFingerprint();
        String changedFingerprint =
                PrimaryKeyIndexDefinitions.create(schema(changed))
                        .definitions()
                        .get(0)
                        .definitionFingerprint();

        assertThat(fingerprint).hasSize(64).isEqualTo(reorderedFingerprint);
        assertThat(changedFingerprint).isNotEqualTo(fingerprint);
    }

    @Test
    void testLegacyVectorCompactionOptionsAreIgnored() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.PK_VECTOR_INDEX_COLUMNS.key(), "embedding");
        options.put("fields.embedding.pk-vector.index.type", "ivf-pq");
        options.put("pk-vector.index.compaction.level-fanout", "9");
        options.put("pk-vector.index.compaction.stale-ratio-threshold", "0.8");

        PrimaryKeyIndexDefinition definition =
                PrimaryKeyIndexDefinitions.create(schema(options)).definitions().get(0);

        assertThat(definition.compactionLevelFanout()).isEqualTo(5);
        assertThat(definition.compactionStaleRatioThreshold()).isEqualTo(0.2);
    }

    @Test
    void testRejectsDuplicateColumnWithinFamily() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.PK_BTREE_INDEX_COLUMNS.key(), "name,name");

        assertThatThrownBy(() -> PrimaryKeyIndexDefinitions.create(schema(options)))
                .hasMessageContaining("pk-btree.index.columns")
                .hasMessageContaining("duplicate")
                .hasMessageContaining("name");
    }

    @Test
    void testRejectsColumnConfiguredByMultipleFamilies() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.PK_BTREE_INDEX_COLUMNS.key(), "name");
        options.put(CoreOptions.PK_BITMAP_INDEX_COLUMNS.key(), "name");

        assertThatThrownBy(() -> PrimaryKeyIndexDefinitions.create(schema(options)))
                .hasMessageContaining("name")
                .hasMessageContaining("at most one primary-key index");
    }

    private static TableSchema schema(Map<String, String> options) {
        return new TableSchema(
                0,
                Arrays.asList(
                        new DataField(0, "id", DataTypes.INT().notNull()),
                        new DataField(1, "name", DataTypes.STRING()),
                        new DataField(2, "status", DataTypes.INT()),
                        new DataField(3, "embedding", DataTypes.VECTOR(3, DataTypes.FLOAT()))),
                3,
                Collections.emptyList(),
                Collections.singletonList("id"),
                options,
                "");
    }
}
