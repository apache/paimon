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
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.columnar.BytesColumnVector;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.LongColumnVector;
import org.apache.paimon.data.columnar.MapColumnVector;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.data.columnar.heap.HeapArrayVector;
import org.apache.paimon.data.columnar.heap.HeapIntVector;
import org.apache.paimon.data.columnar.heap.HeapLongVector;
import org.apache.paimon.data.columnar.heap.HeapMapVector;
import org.apache.paimon.data.columnar.heap.HeapRowVector;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MapSharedShreddingReadPlan}. */
class MapSharedShreddingReadPlanTest {

    @Test
    void testReadProjectedPhysicalRowWithoutOverflowColumn() {
        MapSharedShreddingFieldMeta fieldMeta =
                new MapSharedShreddingFieldMeta(
                        nameToId("a", 0, "b", 1),
                        Collections.emptyMap(),
                        new TreeSet<Integer>(),
                        3,
                        2);
        HeapRowVector physicalMap =
                rowVector(
                        fieldMapping(0, -1, 1), longVector(10L), longVector(null), longVector(20L));

        InternalMap restored = readMap(fieldMeta, physicalMap);

        assertThat(restored.size()).isEqualTo(2);
        assertThat(restored.keyArray().getString(0)).isEqualTo(BinaryString.fromString("a"));
        assertThat(restored.keyArray().getString(1)).isEqualTo(BinaryString.fromString("b"));
        assertThat(restored.valueArray().getLong(0)).isEqualTo(10L);
        assertThat(restored.valueArray().getLong(1)).isEqualTo(20L);
    }

    @Test
    void testReadOverflowOnlyWhenOverflowColumnExists() {
        MapSharedShreddingFieldMeta fieldMeta =
                new MapSharedShreddingFieldMeta(
                        nameToId("a", 0, "overflowed", 1),
                        Collections.emptyMap(),
                        new TreeSet<Integer>(Collections.singletonList(1)),
                        1,
                        1);
        HeapRowVector physicalMap =
                rowVector(fieldMapping(-1), longVector(null), overflowMap(1, 30L));

        InternalMap restored = readMap(fieldMeta, physicalMap);

        assertThat(restored.size()).isEqualTo(1);
        assertThat(restored.keyArray().getString(0))
                .isEqualTo(BinaryString.fromString("overflowed"));
        assertThat(restored.valueArray().getLong(0)).isEqualTo(30L);
    }

    @Test
    void testAssembledMapVectorExposesKeyValueChildren() {
        MapSharedShreddingFieldMeta fieldMeta =
                new MapSharedShreddingFieldMeta(
                        nameToId("a", 0, "b", 1),
                        Collections.emptyMap(),
                        new TreeSet<Integer>(),
                        3,
                        2);
        HeapRowVector physicalMap =
                rowVector(
                        fieldMapping(0, 1, -1), longVector(10L), longVector(null), longVector(20L));

        MapColumnVector mapVector = assembleMapVector(fieldMeta, physicalMap);
        ColumnVector[] children = mapVector.getChildren();

        assertThat(children).hasSize(2);
        assertThat(BinaryString.fromBytes(((BytesColumnVector) children[0]).getBytes(0).getBytes()))
                .isEqualTo(BinaryString.fromString("a"));
        assertThat(BinaryString.fromBytes(((BytesColumnVector) children[0]).getBytes(1).getBytes()))
                .isEqualTo(BinaryString.fromString("b"));
        assertThat(((LongColumnVector) children[1]).getLong(0)).isEqualTo(10L);
        assertThat(children[1].isNullAt(1)).isTrue();
        assertThat(mapVector.getMap(0).size()).isEqualTo(2);
    }

    private static InternalMap readMap(
            MapSharedShreddingFieldMeta fieldMeta, HeapRowVector physicalMap) {
        return assembleMapVector(fieldMeta, physicalMap).getMap(0);
    }

    private static MapColumnVector assembleMapVector(
            MapSharedShreddingFieldMeta fieldMeta, HeapRowVector physicalMap) {
        Map<String, MapSharedShreddingFieldMeta> fieldMetas = new LinkedHashMap<>();
        fieldMetas.put("metrics", fieldMeta);
        MapSharedShreddingReadPlan readPlan =
                new MapSharedShreddingReadPlan(logicalType(), fieldMetas);
        VectorizedColumnBatch physicalBatch =
                new VectorizedColumnBatch(new ColumnVector[] {physicalMap});
        physicalBatch.setNumRows(1);
        VectorizedColumnBatch logicalBatch = readPlan.batchAssembler().assemble(physicalBatch);
        return (MapColumnVector) logicalBatch.columns[0];
    }

    private static RowType logicalType() {
        return DataTypes.ROW(
                DataTypes.FIELD(
                        0,
                        "metrics",
                        DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.BIGINT())));
    }

    private static HeapRowVector rowVector(ColumnVector... children) {
        HeapRowVector vector = new HeapRowVector(1, children);
        vector.appendRow();
        return vector;
    }

    private static HeapArrayVector fieldMapping(int... ids) {
        HeapIntVector child = new HeapIntVector(ids.length);
        for (int id : ids) {
            child.appendInt(id);
        }
        HeapArrayVector vector = new HeapArrayVector(1, child);
        vector.putOffsetLength(0, 0, ids.length);
        return vector;
    }

    private static HeapLongVector longVector(Long value) {
        HeapLongVector vector = new HeapLongVector(1);
        if (value == null) {
            vector.appendNull();
        } else {
            vector.appendLong(value);
        }
        return vector;
    }

    private static HeapMapVector overflowMap(int key, Long value) {
        HeapIntVector keys = new HeapIntVector(1);
        keys.appendInt(key);
        HeapLongVector values = longVector(value);
        HeapMapVector vector = new HeapMapVector(1, keys, values);
        vector.putOffsetLength(0, 0, 1);
        return vector;
    }

    private static Map<String, Integer> nameToId(Object... pairs) {
        Map<String, Integer> result = new TreeMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            result.put((String) pairs[i], (Integer) pairs[i + 1]);
        }
        return result;
    }
}
