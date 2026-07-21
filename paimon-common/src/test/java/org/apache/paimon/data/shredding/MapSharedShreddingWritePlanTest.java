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

import org.apache.paimon.CoreOptions.MapSharedShreddingColumnPlacementPolicy;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MapSharedShreddingWritePlan}. */
class MapSharedShreddingWritePlanTest {

    @Test
    void testConvertAndBuildFieldMetadata() {
        RowType logicalType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(
                                1, "tags", DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())));
        MapSharedShreddingWritePlan writePlan =
                new MapSharedShreddingWritePlan(
                        logicalType,
                        Collections.singletonMap("tags", 4),
                        Collections.singletonMap(
                                "tags", MapSharedShreddingColumnPlacementPolicy.PLAIN));

        InternalRow physicalRow =
                writePlan.toPhysicalRow(
                        GenericRow.of(1, stringKeyMap("a", 10L, "b", 20L, "c", 30L)));
        InternalRow physicalTags = physicalRow.getRow(1, 6);

        assertThat(writePlan.logicalRowType()).isEqualTo(logicalType);
        assertThat(writePlan.physicalRowType().getFieldCount()).isEqualTo(2);
        assertThat(physicalRow.getInt(0)).isEqualTo(1);
        assertThat(physicalTags.getArray(0).toIntArray()).containsExactly(0, 1, 2, -1);
        assertThat(physicalTags.getLong(1)).isEqualTo(10L);
        assertThat(physicalTags.getLong(2)).isEqualTo(20L);
        assertThat(physicalTags.getLong(3)).isEqualTo(30L);
        assertThat(physicalTags.isNullAt(4)).isTrue();
        assertThat(physicalTags.isNullAt(5)).isTrue();

        Map<String, Map<String, String>> fieldMetadata = writePlan.fieldMetadata("none");
        assertThat(fieldMetadata).containsOnlyKeys("tags");
        assertThat(fieldMetadata.get("tags"))
                .containsEntry(MapSharedShreddingDefine.FIELD_DICT_COMPRESSION, "none");
        MapSharedShreddingFieldMeta fieldMeta =
                MapSharedShreddingUtils.deserializeMetadata(fieldMetadata.get("tags"));
        assertThat(fieldMeta.nameToId())
                .containsEntry("a", 0)
                .containsEntry("b", 1)
                .containsEntry("c", 2);
        assertThat(fieldMeta.numColumns()).isEqualTo(4);
        assertThat(fieldMeta.maxRowWidth()).isEqualTo(3);
    }

    @Test
    void testFactoryInfersColumnCountFromFirstRow() {
        RowType logicalType =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                0, "tags", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())));
        MapSharedShreddingWritePlanFactory factory = createFactory(logicalType, 4);

        assertThat(factory.shouldCreateWritePlan()).isTrue();
        assertThat(factory.shouldInferWritePlan()).isTrue();
        assertThat(factory.inferBufferRowCount()).isEqualTo(1);
        assertThat(
                        factory.createWritePlan(
                                        Collections.singletonList(
                                                GenericRow.of(
                                                        stringKeyMap("a", 1, "b", 2, "c", 3))))
                                .physicalRowType())
                .isEqualTo(
                        MapSharedShreddingUtils.logicalToPhysicalSchema(
                                logicalType, Collections.singletonMap("tags", 3)));
    }

    @Test
    void testFactoryCapsInferredColumnCountAtMaxColumns() {
        RowType logicalType =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                0, "tags", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())));
        MapSharedShreddingWritePlanFactory factory = createFactory(logicalType, 2);

        assertThat(
                        factory.createWritePlan(
                                        Collections.singletonList(
                                                GenericRow.of(
                                                        stringKeyMap("a", 1, "b", 2, "c", 3))))
                                .physicalRowType())
                .isEqualTo(
                        MapSharedShreddingUtils.logicalToPhysicalSchema(
                                logicalType, Collections.singletonMap("tags", 2)));
    }

    @Test
    void testFactoryUsesConfiguredColumnPlacementPolicy() {
        RowType logicalType =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                0, "tags", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())));
        MapSharedShreddingWritePlanFactory factory = createFactory(logicalType, 3, "sequential");
        InternalRow first = GenericRow.of(stringKeyMap("a", 1, "b", 2, "c", 6));
        InternalRow second = GenericRow.of(stringKeyMap("b", 3, "d", 4, "a", 5));
        ShreddingWritePlan writePlan = factory.createWritePlan(Collections.singletonList(first));

        writePlan.toPhysicalRow(first).getRow(0, 5);
        InternalRow physicalMap = writePlan.toPhysicalRow(second).getRow(0, 5);

        assertThat(physicalMap.getArray(0).toIntArray()).containsExactly(0, 1, 3);
        assertThat(physicalMap.getInt(1)).isEqualTo(5);
        assertThat(physicalMap.getInt(2)).isEqualTo(3);
        assertThat(physicalMap.getInt(3)).isEqualTo(4);
    }

    @Test
    void testFactoryUsesLruColumnPlacementByDefault() {
        RowType logicalType =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                0, "tags", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())));
        MapSharedShreddingWritePlanFactory factory = createFactory(logicalType, 3);
        InternalRow first = GenericRow.of(stringKeyMap("a", 10, "b", 20, "c", 30));
        ShreddingWritePlan writePlan = factory.createWritePlan(Collections.singletonList(first));

        writePlan.toPhysicalRow(first).getRow(0, 5);
        writePlan.toPhysicalRow(GenericRow.of(stringKeyMap("a", 40, "b", 50))).getRow(0, 5);
        writePlan
                .toPhysicalRow(GenericRow.of(stringKeyMap("d", 60, "e", 70, "f", 80)))
                .getRow(0, 5);
        InternalRow physicalMap =
                writePlan
                        .toPhysicalRow(
                                GenericRow.of(stringKeyMap("a", 90, "d", 100, "e", 110, "f", 120)))
                        .getRow(0, 5);

        assertThat(physicalMap.getArray(0).toIntArray()).containsExactly(4, 5, 3);
        assertThat(physicalMap.getInt(1)).isEqualTo(110);
        assertThat(physicalMap.getInt(2)).isEqualTo(120);
        assertThat(physicalMap.getInt(3)).isEqualTo(100);
        assertThat(physicalMap.getMap(4)).isEqualTo(intKeyMap(0, 90));
    }

    private static MapSharedShreddingWritePlanFactory createFactory(
            RowType logicalType, int maxColumns) {
        return createFactory(logicalType, maxColumns, null);
    }

    private static MapSharedShreddingWritePlanFactory createFactory(
            RowType logicalType, int maxColumns, String placementPolicy) {
        Options options = new Options();
        options.setString("fields.tags.map.storage-layout", "shared-shredding");
        options.setString(
                "fields.tags.map.shared-shredding.max-columns", String.valueOf(maxColumns));
        if (placementPolicy != null) {
            options.setString(
                    "fields.tags.map.shared-shredding.column-placement-policy", placementPolicy);
        }
        return new MapSharedShreddingWritePlanFactory(logicalType, options);
    }

    private static GenericMap stringKeyMap(Object... keyValues) {
        Map<Object, Object> values = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            values.put(BinaryString.fromString((String) keyValues[i]), keyValues[i + 1]);
        }
        return new GenericMap(values);
    }

    private static GenericMap intKeyMap(Object... keyValues) {
        Map<Object, Object> values = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            values.put(keyValues[i], keyValues[i + 1]);
        }
        return new GenericMap(values);
    }
}
