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

package org.apache.paimon.flink.lakestream;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataField;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.CatalogTable;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for lake-stream schema conversion and compatibility checks. */
public class LakeStreamUtilsTest {

    @Test
    public void testStoreTableIsConvertedToPaimonSchema() {
        Map<String, String> options = new HashMap<>();
        options.put("paimon." + CoreOptions.MERGE_ENGINE.key(), "deduplicate");
        options.put("bucket.num", "7");
        options.put("bucket.key", "id");
        options.put("scan.startup.mode", "earliest");
        CatalogTable storeTable =
                storeTable(
                        org.apache.flink.table.api.Schema.newBuilder()
                                .column("id", DataTypes.INT().notNull())
                                .primaryKey("id")
                                .build(),
                        Collections.singletonList("id"),
                        options);

        Schema converted = LakeStreamUtils.toPaimonSchema(storeTable);

        assertThat(converted.rowType().getFieldNames()).containsExactly("id");
        assertThat(
                        converted
                                .fields()
                                .get(0)
                                .type()
                                .equalsIgnoreFieldId(
                                        org.apache.paimon.types.DataTypes.INT().notNull()))
                .isTrue();
        assertThat(converted.primaryKeys()).containsExactly("id");
        assertThat(converted.partitionKeys()).containsExactly("id");
        assertThat(converted.options())
                .containsOnlyKeys(CoreOptions.MERGE_ENGINE.key())
                .containsEntry(CoreOptions.MERGE_ENGINE.key(), "deduplicate");
        assertThat(converted.comment()).isEqualTo("store comment");
    }

    @Test
    public void testCompatibilityIncludesPrimaryKeys() {
        Schema paimon =
                paimonSchema(
                        Collections.singletonList(
                                new DataField(
                                        0,
                                        "id",
                                        org.apache.paimon.types.DataTypes.INT().notNull())),
                        Collections.singletonList("id"),
                        Collections.emptyList(),
                        Collections.emptyMap());
        CatalogTable store =
                storeTable(
                        org.apache.flink.table.api.Schema.newBuilder()
                                .column("id", DataTypes.INT().notNull())
                                .build(),
                        Collections.emptyList(),
                        Collections.emptyMap());

        assertThat(LakeStreamUtils.isCompatible(paimon, LakeStreamUtils.toPaimonSchema(store)))
                .isFalse();
    }

    @Test
    public void testCompatibilityIncludesPartitionKeys() {
        Schema paimon =
                paimonSchema(
                        Collections.singletonList(
                                new DataField(0, "id", org.apache.paimon.types.DataTypes.INT())),
                        Collections.emptyList(),
                        Collections.singletonList("id"),
                        Collections.emptyMap());
        CatalogTable store =
                storeTable(
                        org.apache.flink.table.api.Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .build(),
                        Collections.emptyList(),
                        Collections.emptyMap());

        assertThat(LakeStreamUtils.isCompatible(paimon, LakeStreamUtils.toPaimonSchema(store)))
                .isFalse();
    }

    @Test
    public void testCompatibilityIncludesSharedPaimonOptions() {
        Map<String, String> paimonOptions = new HashMap<>();
        paimonOptions.put(CoreOptions.MERGE_ENGINE.key(), "deduplicate");
        Schema paimon =
                paimonSchema(
                        Collections.singletonList(
                                new DataField(0, "id", org.apache.paimon.types.DataTypes.INT())),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        paimonOptions);
        Map<String, String> storeOptions = new HashMap<>();
        storeOptions.put("paimon." + CoreOptions.MERGE_ENGINE.key(), "aggregation");
        CatalogTable store =
                storeTable(
                        org.apache.flink.table.api.Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .build(),
                        Collections.emptyList(),
                        storeOptions);

        assertThat(LakeStreamUtils.isCompatible(paimon, LakeStreamUtils.toPaimonSchema(store)))
                .isFalse();
    }

    @Test
    public void testCompatibilityIncludesNullability() {
        Schema paimon =
                paimonSchema(
                        Collections.singletonList(
                                new DataField(
                                        0,
                                        "id",
                                        org.apache.paimon.types.DataTypes.INT().notNull())),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap());
        CatalogTable store =
                storeTable(
                        org.apache.flink.table.api.Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .build(),
                        Collections.emptyList(),
                        Collections.emptyMap());

        assertThat(LakeStreamUtils.isCompatible(paimon, LakeStreamUtils.toPaimonSchema(store)))
                .isFalse();
    }

    @Test
    public void testCompatibilityIgnoresNonSharedAttributes() {
        Map<String, String> paimonOptions = new HashMap<>();
        paimonOptions.put(CoreOptions.BUCKET.key(), "1");
        paimonOptions.put(CoreOptions.SCAN_MODE.key(), "latest");
        Schema paimon =
                paimonSchema(
                        Collections.singletonList(
                                new DataField(
                                        7,
                                        "id",
                                        org.apache.paimon.types.DataTypes.INT(),
                                        "paimon field",
                                        "0")),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        paimonOptions);
        Map<String, String> storeOptions = new HashMap<>();
        storeOptions.put("bucket.num", "9");
        storeOptions.put("bucket.key", "id");
        storeOptions.put("scan.startup.mode", "earliest");
        CatalogTable store =
                storeTable(
                        org.apache.flink.table.api.Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .withComment("store field")
                                .build(),
                        Collections.emptyList(),
                        storeOptions);

        assertThat(LakeStreamUtils.isCompatible(paimon, LakeStreamUtils.toPaimonSchema(store)))
                .isTrue();
    }

    private static Schema paimonSchema(
            java.util.List<DataField> fields,
            java.util.List<String> primaryKeys,
            java.util.List<String> partitionKeys,
            Map<String, String> options) {
        return new Schema(fields, partitionKeys, primaryKeys, options, "paimon comment");
    }

    private static CatalogTable storeTable(
            org.apache.flink.table.api.Schema schema,
            java.util.List<String> partitionKeys,
            Map<String, String> options) {
        return CatalogTable.newBuilder()
                .schema(schema)
                .partitionKeys(partitionKeys)
                .options(options)
                .comment("store comment")
                .build();
    }
}
