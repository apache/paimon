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

package org.apache.paimon.data.shredding;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link MapSharedShreddingUtils}. */
class MapSharedShreddingUtilsTest {

    @Test
    void testIsShreddingKeyMap() {
        assertThat(
                        MapSharedShreddingUtils.isShreddingKeyMap(
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())))
                .isTrue();
        assertThat(
                        MapSharedShreddingUtils.isShreddingKeyMap(
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.DOUBLE())))
                .isTrue();
        assertThat(
                        MapSharedShreddingUtils.isShreddingKeyMap(
                                DataTypes.MAP(
                                        DataTypes.STRING(),
                                        DataTypes.ROW(
                                                DataTypes.FIELD(0, "x", DataTypes.INT()),
                                                DataTypes.FIELD(1, "y", DataTypes.STRING())))))
                .isTrue();
        assertThat(
                        MapSharedShreddingUtils.isShreddingKeyMap(
                                DataTypes.MAP(DataTypes.INT(), DataTypes.STRING())))
                .isFalse();
        assertThat(MapSharedShreddingUtils.isShreddingKeyMap(DataTypes.INT())).isFalse();
        assertThat(MapSharedShreddingUtils.isShreddingKeyMap(DataTypes.ARRAY(DataTypes.STRING())))
                .isFalse();
    }

    @Test
    void testDetectAndBuildColumnToNumColumns() {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(
                                1, "tags", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())),
                        DataTypes.FIELD(
                                2,
                                "metrics",
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())),
                        DataTypes.FIELD(
                                3, "codes", DataTypes.MAP(DataTypes.INT(), DataTypes.STRING())));

        Options conf = new Options();
        conf.setString("fields.tags.map.storage-layout", "shared-shredding");
        conf.setString("fields.metrics.map.storage-layout", "shared-shredding");
        conf.setString("fields.tags.map.shared-shredding.max-columns", "128");
        conf.setString("fields.metrics.map.shared-shredding.max-columns", "64");
        conf.setString("fields.codes.map.storage-layout", "shared-shredding");

        CoreOptions options = new CoreOptions(conf);
        assertThat(MapSharedShreddingUtils.detectShreddingColumns(rowType, options))
                .containsExactly("tags", "metrics");
        assertThat(
                        MapSharedShreddingUtils.buildColumnToNumColumns(
                                MapSharedShreddingUtils.detectShreddingColumns(rowType, options),
                                options))
                .containsEntry("tags", 128)
                .containsEntry("metrics", 64);

        Options defaultConf = new Options();
        CoreOptions defaultOptions = new CoreOptions(defaultConf);
        assertThat(MapSharedShreddingUtils.detectShreddingColumns(rowType, defaultOptions))
                .isEmpty();
        assertThat(
                        MapSharedShreddingUtils.buildColumnToNumColumns(
                                Arrays.asList("tags"), defaultOptions))
                .containsEntry("tags", 256);
    }

    @Test
    void testLogicalToPhysicalSchema() {
        RowType logical =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(
                                1,
                                "metrics",
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.DOUBLE().notNull())));

        Map<String, Integer> fieldToNumColumns = new HashMap<>();
        fieldToNumColumns.put("metrics", 2);

        RowType physical =
                MapSharedShreddingUtils.logicalToPhysicalSchema(logical, fieldToNumColumns);
        DataField metrics = physical.getField("metrics");
        assertThat(metrics.id()).isEqualTo(1);
        assertThat(metrics.type()).isInstanceOf(RowType.class);
        RowType metricsPhysicalType = (RowType) metrics.type();
        assertThat(metricsPhysicalType.getFieldNames())
                .containsExactly("__field_mapping", "__col_0", "__col_1", "__overflow");
        assertThat(metricsPhysicalType.getFields())
                .extracting(DataField::id)
                .containsExactly(0, 1, 2, 3);
        assertThat(metricsPhysicalType.getField("__col_0").type())
                .isEqualTo(DataTypes.DOUBLE().notNull());
        assertThat(metricsPhysicalType.getField("__overflow").type())
                .isEqualTo(DataTypes.MAP(DataTypes.INT(), DataTypes.DOUBLE().notNull()));
    }

    @Test
    void testLogicalToPhysicalSchemaNestedValueAndStableFieldIds() {
        RowType nestedValue =
                DataTypes.ROW(
                        DataTypes.FIELD(2, "a", DataTypes.INT()),
                        DataTypes.FIELD(3, "b", DataTypes.STRING()));
        RowType original =
                DataTypes.ROW(
                        DataTypes.FIELD(1, "data", DataTypes.MAP(DataTypes.STRING(), nestedValue)));
        RowType evolved =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "new_col", DataTypes.STRING()),
                        DataTypes.FIELD(1, "data", DataTypes.MAP(DataTypes.STRING(), nestedValue)));

        Map<String, Integer> fieldToNumColumns = new HashMap<>();
        fieldToNumColumns.put("data", 2);

        RowType physical =
                MapSharedShreddingUtils.logicalToPhysicalSchema(original, fieldToNumColumns);
        assertThat(physical.getField("data").id()).isEqualTo(1);
        RowType dataPhysicalType = (RowType) physical.getField("data").type();
        assertThat(dataPhysicalType.getFieldNames())
                .containsExactly("__field_mapping", "__col_0", "__col_1", "__overflow");
        assertThat(dataPhysicalType.getFields())
                .extracting(DataField::id)
                .containsExactly(0, 1, 2, 3);
        assertThat(dataPhysicalType.getField("__col_0").type()).isEqualTo(nestedValue);
        assertThat(dataPhysicalType.getField("__overflow").type())
                .isEqualTo(DataTypes.MAP(DataTypes.INT(), nestedValue));

        RowType evolvedPhysical =
                MapSharedShreddingUtils.logicalToPhysicalSchema(evolved, fieldToNumColumns);
        assertThat(evolvedPhysical.getField("data").id()).isEqualTo(1);
        assertThat(((RowType) evolvedPhysical.getField("data").type()).getFields())
                .extracting(DataField::id)
                .containsExactly(0, 1, 2, 3);
    }

    @Test
    void testLogicalToPhysicalSchemaNoShreddingColumns() {
        RowType logical =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(1, "name", DataTypes.STRING()));

        assertThat(MapSharedShreddingUtils.logicalToPhysicalSchema(logical, new HashMap<>()))
                .isEqualTo(logical);
    }

    @Test
    void testGetPhysicalColumnIndices() {
        MapSharedShreddingFieldMeta fieldMeta =
                new MapSharedShreddingFieldMeta(
                        nameToId("age", 0, "name", 1),
                        fieldToColumns(0, Arrays.asList(0, 2), 1, Arrays.asList(1)),
                        new HashSet<>(),
                        3,
                        2);

        assertThat(MapSharedShreddingUtils.getPhysicalColumnIndices(fieldMeta, "age"))
                .containsExactly(0, 2);
        assertThat(MapSharedShreddingUtils.getPhysicalColumnIndices(fieldMeta, "name"))
                .containsExactly(1);

        assertThatThrownBy(
                        () -> MapSharedShreddingUtils.getPhysicalColumnIndices(fieldMeta, "score"))
                .hasMessageContaining("cannot find field score in map shared shredding meta");

        MapSharedShreddingFieldMeta missingColumnsMeta =
                new MapSharedShreddingFieldMeta(
                        nameToId("age", 0), new TreeMap<>(), new HashSet<>(), 3, 2);
        assertThatThrownBy(
                        () ->
                                MapSharedShreddingUtils.getPhysicalColumnIndices(
                                        missingColumnsMeta, "age"))
                .hasMessageContaining(
                        "cannot find field id 0 in field_to_columns in map shared shredding meta");
    }

    @Test
    void testIsOverflowField() {
        MapSharedShreddingFieldMeta fieldMeta =
                new MapSharedShreddingFieldMeta(
                        nameToId("age", 0, "name", 1),
                        fieldToColumns(0, Arrays.asList(0), 1, Arrays.asList(1)),
                        new TreeSet<>(Arrays.asList(1)),
                        2,
                        2);

        assertThat(MapSharedShreddingUtils.isOverflowField(fieldMeta, "age")).isFalse();
        assertThat(MapSharedShreddingUtils.isOverflowField(fieldMeta, "name")).isTrue();
        assertThatThrownBy(() -> MapSharedShreddingUtils.isOverflowField(fieldMeta, "score"))
                .hasMessageContaining("cannot find field score in map shared shredding meta");
    }

    @Test
    void testBuildSpecificPhysicalStructType() {
        Set<Integer> physicalColumnIds = new HashSet<>(Arrays.asList(3, 1));

        RowType physicalType =
                (RowType)
                        MapSharedShreddingUtils.buildSpecificPhysicalStructType(
                                DataTypes.BIGINT().notNull(), physicalColumnIds, true);
        assertThat(physicalType.getFieldNames())
                .containsExactly("__field_mapping", "__col_1", "__col_3", "__overflow");
        assertThat(physicalType.getFields()).extracting(DataField::id).containsExactly(0, 1, 2, 3);
        assertThat(physicalType.getField("__field_mapping").type())
                .isEqualTo(DataTypes.ARRAY(DataTypes.INT()));
        assertThat(physicalType.getField("__col_1").type())
                .isEqualTo(DataTypes.BIGINT().notNull());
        assertThat(physicalType.getField("__col_3").type())
                .isEqualTo(DataTypes.BIGINT().notNull());
        assertThat(physicalType.getField("__overflow").type())
                .isEqualTo(DataTypes.MAP(DataTypes.INT(), DataTypes.BIGINT().notNull()));

        RowType physicalTypeWithoutOverflow =
                (RowType)
                        MapSharedShreddingUtils.buildSpecificPhysicalStructType(
                                DataTypes.BIGINT(), physicalColumnIds, false);
        assertThat(physicalTypeWithoutOverflow.getFieldNames())
                .containsExactly("__field_mapping", "__col_1", "__col_3");
    }

    @Test
    void testMetadataRoundtrip() {
        Map<String, Integer> nameToId = new TreeMap<>();
        nameToId.put("age", 0);
        nameToId.put("name", 1);

        Map<Integer, List<Integer>> fieldToColumns = new TreeMap<>();
        fieldToColumns.put(0, Arrays.asList(0));
        fieldToColumns.put(1, Arrays.asList(1, 2));

        HashSet<Integer> overflowSet = new HashSet<>();
        overflowSet.add(1);
        overflowSet.add(5);

        MapSharedShreddingFieldMeta original =
                new MapSharedShreddingFieldMeta(nameToId, fieldToColumns, overflowSet, 3, 2);

        Map<String, String> metadata = new HashMap<>();
        MapSharedShreddingUtils.serializeMetadata(original, "none", metadata);

        assertThat(MapSharedShreddingUtils.hasShreddingMetadata(metadata)).isTrue();
        assertThat(metadata.get(MapShreddingDefine.STORAGE_LAYOUT)).isEqualTo("shared-shredding");
        assertThat(metadata.get(MapSharedShreddingDefine.VERSION)).isEqualTo("1");
        assertThat(metadata.get(MapSharedShreddingDefine.NUM_COLUMNS)).isEqualTo("3");
        assertThat(metadata.get(MapSharedShreddingDefine.MAX_ROW_WIDTH)).isEqualTo("2");

        String expectedDict = "{\"age\":0,\"name\":1}";
        assertThat(metadata.get(MapSharedShreddingDefine.FIELD_DICT)).isEqualTo(expectedDict);
        assertThat(metadata.get(MapSharedShreddingDefine.FIELD_DICT_ORIGINAL_SIZE))
                .isEqualTo(String.valueOf(expectedDict.length()));
        assertThat(metadata.get(MapSharedShreddingDefine.FIELD_COLUMNS))
                .isEqualTo("{\"0\":[0],\"1\":[1,2]}");
        assertThat(metadata.get(MapSharedShreddingDefine.OVERFLOW_SET)).isEqualTo("[1,5]");
        assertThat(MapSharedShreddingUtils.deserializeMetadata(metadata, "none"))
                .isEqualTo(original);
    }

    @Test
    void testMetadataRoundtripCompression() {
        Map<String, Integer> nameToId = new TreeMap<>();
        nameToId.put("alpha", 0);
        nameToId.put("beta", 1);
        nameToId.put("gamma", 2);

        Map<Integer, List<Integer>> fieldToColumns = new TreeMap<>();
        fieldToColumns.put(0, Arrays.asList(0, 1, 2));
        fieldToColumns.put(1, Arrays.asList(3));
        fieldToColumns.put(2, Arrays.asList(4, 5));

        HashSet<Integer> overflowSet = new HashSet<>();
        overflowSet.add(2);
        MapSharedShreddingFieldMeta original =
                new MapSharedShreddingFieldMeta(nameToId, fieldToColumns, overflowSet, 6, 3);

        for (String compression : Arrays.asList("none", "lz4", "zstd")) {
            Map<String, String> metadata = new HashMap<>();
            MapSharedShreddingUtils.serializeMetadata(original, compression, metadata);
            assertThat(MapSharedShreddingUtils.deserializeMetadata(metadata, compression))
                    .isEqualTo(original);
        }
    }

    @Test
    void testMetadataRoundtripEmptyData() {
        MapSharedShreddingFieldMeta original =
                new MapSharedShreddingFieldMeta(
                        new TreeMap<>(), new TreeMap<>(), new HashSet<>(), 0, 0);

        for (String compression : Arrays.asList("none", "lz4", "zstd")) {
            Map<String, String> metadata = new HashMap<>();
            MapSharedShreddingUtils.serializeMetadata(original, compression, metadata);
            assertThat(MapSharedShreddingUtils.deserializeMetadata(metadata, compression))
                    .isEqualTo(original);
        }
    }

    @Test
    void testDeserializeMetadataErrors() {
        assertThatThrownBy(() -> MapSharedShreddingUtils.deserializeMetadata(null, "none"))
                .hasMessageContaining("metadata is null or storage layout is not shared-shredding");

        Map<String, String> missingLayout = new HashMap<>();
        missingLayout.put("some_key", "some_value");
        assertThatThrownBy(() -> MapSharedShreddingUtils.deserializeMetadata(missingLayout, "none"))
                .hasMessageContaining("metadata is null or storage layout is not shared-shredding");

        Map<String, String> metadata = new HashMap<>();
        metadata.put(MapShreddingDefine.STORAGE_LAYOUT, "default");
        assertThatThrownBy(() -> MapSharedShreddingUtils.deserializeMetadata(metadata, "none"))
                .hasMessageContaining("metadata is null or storage layout is not shared-shredding");

        Map<String, String> missingVersion = new HashMap<>();
        missingVersion.put(
                MapShreddingDefine.STORAGE_LAYOUT,
                MapShreddingDefine.STORAGE_LAYOUT_SHARED_SHREDDING);
        assertThatThrownBy(
                        () -> MapSharedShreddingUtils.deserializeMetadata(missingVersion, "none"))
                .hasMessageContaining(
                        "missing shredding metadata key: paimon.map.shared-shredding.version");

        Map<String, String> wrongVersion = new HashMap<>();
        wrongVersion.put(
                MapShreddingDefine.STORAGE_LAYOUT,
                MapShreddingDefine.STORAGE_LAYOUT_SHARED_SHREDDING);
        wrongVersion.put(MapSharedShreddingDefine.VERSION, "999");
        wrongVersion.put(MapSharedShreddingDefine.FIELD_DICT_ORIGINAL_SIZE, "2");
        wrongVersion.put(MapSharedShreddingDefine.FIELD_DICT, "{}");
        assertThatThrownBy(() -> MapSharedShreddingUtils.deserializeMetadata(wrongVersion, "none"))
                .hasMessageContaining("unsupported shared-shredding metadata version: 999");

        Map<String, String> missingFieldDict = new HashMap<>();
        missingFieldDict.put(
                MapShreddingDefine.STORAGE_LAYOUT,
                MapShreddingDefine.STORAGE_LAYOUT_SHARED_SHREDDING);
        missingFieldDict.put(MapSharedShreddingDefine.VERSION, "1");
        missingFieldDict.put(MapSharedShreddingDefine.FIELD_DICT_ORIGINAL_SIZE, "2");
        assertThatThrownBy(
                        () -> MapSharedShreddingUtils.deserializeMetadata(missingFieldDict, "none"))
                .hasMessageContaining(
                        "missing shredding metadata key: paimon.map.shared-shredding.field-dict");
    }

    @Test
    void testHasShreddingMetadata() {
        assertThat(MapSharedShreddingUtils.hasShreddingMetadata(null)).isFalse();

        Map<String, String> metadata = new HashMap<>();
        metadata.put(
                MapShreddingDefine.STORAGE_LAYOUT,
                MapShreddingDefine.STORAGE_LAYOUT_SHARED_SHREDDING);
        assertThat(MapSharedShreddingUtils.hasShreddingMetadata(metadata)).isTrue();

        metadata.put(MapShreddingDefine.STORAGE_LAYOUT, "default");
        assertThat(MapSharedShreddingUtils.hasShreddingMetadata(metadata)).isFalse();

        assertThat(MapSharedShreddingUtils.hasShreddingMetadata(new HashMap<>())).isFalse();
    }

    @Test
    void testPhysicalColumnName() {
        assertThat(MapSharedShreddingDefine.physicalColumnName(0)).isEqualTo("__col_0");
        assertThat(MapSharedShreddingDefine.physicalColumnName(1)).isEqualTo("__col_1");
        assertThat(MapSharedShreddingDefine.physicalColumnName(99)).isEqualTo("__col_99");
    }

    private static Map<String, Integer> nameToId(Object... kvs) {
        Map<String, Integer> map = new TreeMap<>();
        for (int i = 0; i < kvs.length; i += 2) {
            map.put((String) kvs[i], (Integer) kvs[i + 1]);
        }
        return map;
    }

    private static Map<Integer, List<Integer>> fieldToColumns(
            int fieldId, List<Integer> columns) {
        Map<Integer, List<Integer>> map = new TreeMap<>();
        map.put(fieldId, columns);
        return map;
    }

    private static Map<Integer, List<Integer>> fieldToColumns(
            int fieldId0, List<Integer> columns0, int fieldId1, List<Integer> columns1) {
        Map<Integer, List<Integer>> map = fieldToColumns(fieldId0, columns0);
        map.put(fieldId1, columns1);
        return map;
    }
}
