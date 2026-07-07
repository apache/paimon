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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
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
        MapSharedShreddingContext context =
                new MapSharedShreddingContext(Collections.singletonMap("tags", 4));
        MapSharedShreddingWritePlan writePlan =
                new MapSharedShreddingWritePlan(logicalType, context.computeNextK(), context);

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
        MapSharedShreddingFieldMeta fieldMeta =
                MapSharedShreddingUtils.deserializeMetadata(fieldMetadata.get("tags"), "none");
        assertThat(fieldMeta.nameToId()).containsEntry("a", 0).containsEntry("b", 1);
        assertThat(fieldMeta.numColumns()).isEqualTo(4);
        assertThat(fieldMeta.maxRowWidth()).isEqualTo(3);
        assertThat(context.computeNextK()).containsEntry("tags", 3);
    }

    @Test
    void testFactoryCreatesPlanFromContext() {
        RowType logicalType =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                0, "tags", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())));
        MapSharedShreddingContext context =
                new MapSharedShreddingContext(Collections.singletonMap("tags", 2));
        MapSharedShreddingWritePlanFactory factory =
                new MapSharedShreddingWritePlanFactory(logicalType, context);

        assertThat(factory.shouldCreateWritePlan()).isTrue();
        assertThat(factory.shouldInferWritePlan()).isFalse();
        assertThat(factory.createWritePlan(Collections.emptyList()).physicalRowType())
                .isEqualTo(
                        MapSharedShreddingUtils.logicalToPhysicalSchema(
                                logicalType, Collections.singletonMap("tags", 2)));
    }

    private static GenericMap stringKeyMap(Object... keyValues) {
        Map<Object, Object> values = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            values.put(BinaryString.fromString((String) keyValues[i]), keyValues[i + 1]);
        }
        return new GenericMap(values);
    }
}
