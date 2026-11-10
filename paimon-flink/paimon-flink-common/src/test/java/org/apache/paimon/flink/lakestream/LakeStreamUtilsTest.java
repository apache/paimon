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
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.CatalogTable;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

        Schema converted = toPaimonSchema(storeTable);

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
    public void testStoreSchemaWithUnresolvedTypes() {
        CatalogTable store =
                storeTable(
                        org.apache.flink.table.api.Schema.newBuilder()
                                .column("id", "INT NOT NULL")
                                .column("nested", "ARRAY<ROW<item BIGINT>>")
                                .primaryKey("id")
                                .build(),
                        Collections.emptyList(),
                        Collections.emptyMap());

        Schema schema = toPaimonSchema(store);
        assertThat(schema.fields().get(0).type())
                .isEqualTo(org.apache.paimon.types.DataTypes.INT().notNull());
        assertThat(schema.fields().get(1).type()).isInstanceOf(ArrayType.class);
        assertThat(schema.primaryKeys()).containsExactly("id");
    }

    private static Schema toPaimonSchema(CatalogTable table) {
        return LakeStreamUtils.toPaimonSchema(
                table, LakeStreamUtilsTest.class.getClassLoader(), new Configuration());
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

        assertThat(LakeStreamUtils.isCompatible(paimon, toPaimonSchema(store))).isFalse();
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

        assertThat(LakeStreamUtils.isCompatible(paimon, toPaimonSchema(store))).isFalse();
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

        assertThat(LakeStreamUtils.isCompatible(paimon, toPaimonSchema(store))).isFalse();
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

        assertThat(LakeStreamUtils.isCompatible(paimon, toPaimonSchema(store))).isFalse();
    }

    @Test
    public void testCompatibilityIgnoresNestedComments() {
        DataField nested =
                new DataField(
                        1, "x", org.apache.paimon.types.DataTypes.INT(), "paimon nested comment");
        Schema paimon =
                paimonSchema(
                        Collections.singletonList(
                                new DataField(
                                        0,
                                        "r",
                                        new RowType(true, Collections.singletonList(nested)))),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap());
        CatalogTable store =
                storeTable(
                        org.apache.flink.table.api.Schema.newBuilder()
                                .column("r", DataTypes.ROW(DataTypes.FIELD("x", DataTypes.INT())))
                                .build(),
                        Collections.emptyList(),
                        Collections.emptyMap());

        assertThat(LakeStreamUtils.isCompatible(paimon, toPaimonSchema(store))).isTrue();
    }

    @Test
    public void testCompatibilityIgnoresNestedCommentsInsideContainers() {
        DataField nested =
                new DataField(
                        1, "x", org.apache.paimon.types.DataTypes.INT(), "paimon nested comment");
        Schema paimon =
                paimonSchema(
                        Collections.singletonList(
                                new DataField(
                                        0,
                                        "r",
                                        new ArrayType(
                                                true,
                                                new RowType(
                                                        true, Collections.singletonList(nested))))),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap());
        CatalogTable store =
                storeTable(
                        org.apache.flink.table.api.Schema.newBuilder()
                                .column(
                                        "r",
                                        DataTypes.ARRAY(
                                                DataTypes.ROW(
                                                        DataTypes.FIELD("x", DataTypes.INT()))))
                                .build(),
                        Collections.emptyList(),
                        Collections.emptyMap());

        assertThat(LakeStreamUtils.isCompatible(paimon, toPaimonSchema(store))).isTrue();
    }

    @Test
    public void testCompatibilityStillRejectsNestedTypeDrift() {
        DataField nested =
                new DataField(
                        1, "x", org.apache.paimon.types.DataTypes.INT(), "paimon nested comment");
        Schema paimon =
                paimonSchema(
                        Collections.singletonList(
                                new DataField(
                                        0,
                                        "r",
                                        new RowType(true, Collections.singletonList(nested)))),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap());
        CatalogTable store =
                storeTable(
                        org.apache.flink.table.api.Schema.newBuilder()
                                .column(
                                        "r",
                                        DataTypes.ROW(DataTypes.FIELD("x", DataTypes.BIGINT())))
                                .build(),
                        Collections.emptyList(),
                        Collections.emptyMap());

        assertThat(LakeStreamUtils.isCompatible(paimon, toPaimonSchema(store))).isFalse();
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

        assertThat(LakeStreamUtils.isCompatible(paimon, toPaimonSchema(store))).isTrue();
    }

    @Test
    public void testCompatibilityReturnsFalseForOtherTypesWithTheSameTypeRoot() {
        assertThat(
                        LakeStreamUtils.isCompatible(
                                schemaWithType(new RowType(true, Collections.emptyList())),
                                schemaWithType(customType(DataTypeRoot.ROW))))
                .isFalse();
        assertThat(
                        LakeStreamUtils.isCompatible(
                                schemaWithType(
                                        new ArrayType(
                                                true, org.apache.paimon.types.DataTypes.INT())),
                                schemaWithType(customType(DataTypeRoot.ARRAY))))
                .isFalse();
        assertThat(
                        LakeStreamUtils.isCompatible(
                                schemaWithType(
                                        new MultisetType(
                                                true, org.apache.paimon.types.DataTypes.INT())),
                                schemaWithType(customType(DataTypeRoot.MULTISET))))
                .isFalse();
        assertThat(
                        LakeStreamUtils.isCompatible(
                                schemaWithType(
                                        new MapType(
                                                true,
                                                org.apache.paimon.types.DataTypes.INT(),
                                                org.apache.paimon.types.DataTypes.INT())),
                                schemaWithType(customType(DataTypeRoot.MAP))))
                .isFalse();
    }

    private static DataType customType(DataTypeRoot root) {
        DataType type = mock(DataType.class);
        when(type.getTypeRoot()).thenReturn(root);
        when(type.isNullable()).thenReturn(true);
        return type;
    }

    private static Schema schemaWithType(DataType type) {
        return paimonSchema(
                Collections.singletonList(new DataField(0, "r", type)),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyMap());
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
