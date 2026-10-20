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

package org.apache.paimon.flink;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.KeyProjectedRow;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlinkRowData} and vector type. */
public class FlinkRowDataWithVectorTest {

    @Test
    public void testVectorAsArrayData() {
        float[] values = new float[] {1.0f, 2.0f, 3.0f};
        InternalRow row = GenericRow.of(1, BinaryVector.fromPrimitiveArray(values));

        FlinkRowData rowData = new FlinkRowData(row);
        ArrayData arrayData = rowData.getArray(1);

        assertThat(arrayData.toFloatArray()).isEqualTo(values);
    }

    @Test
    public void testBinaryVectorAndArrayAsArrayData() {
        float[] vector = new float[] {0.0f, 1.0f, 2.0f};
        float[] array = new float[] {3.0f, 4.0f};
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.VECTOR(3, DataTypes.FLOAT()), DataTypes.ARRAY(DataTypes.FLOAT()));
        BinaryRow row =
                new InternalRowSerializer(rowType)
                        .toBinaryRow(
                                GenericRow.of(
                                        BinaryVector.fromPrimitiveArray(vector),
                                        new GenericArray(array)));

        FlinkRowData rowData = new FlinkRowData(row, rowType);

        assertThat(rowData.getArray(0).toFloatArray()).isEqualTo(vector);
        assertThat(rowData.getArray(1).toFloatArray()).isEqualTo(array);
    }

    @Test
    public void testProjectedBinaryVectorAsArrayData() {
        float[] vector = new float[] {0.0f, 1.0f, 2.0f};
        RowType valueType = DataTypes.ROW(DataTypes.VECTOR(3, DataTypes.FLOAT()));
        BinaryRow sortedRow =
                new InternalRowSerializer(DataTypes.INT(), valueType.getTypeAt(0))
                        .toBinaryRow(GenericRow.of(5, BinaryVector.fromPrimitiveArray(vector)));
        KeyProjectedRow projected = new KeyProjectedRow(new int[] {1}).replaceRow(sortedRow);

        assertThat(new FlinkRowData(projected, valueType).getArray(0).toFloatArray())
                .isEqualTo(vector);
    }

    @Test
    public void testNestedBinaryVectorsAsArrayData() {
        float[] values = new float[] {0.0f, 1.0f, 2.0f};
        BinaryVector vector = BinaryVector.fromPrimitiveArray(values);
        RowType nestedRowType = DataTypes.ROW(DataTypes.VECTOR(3, DataTypes.FLOAT()));
        RowType rowType =
                DataTypes.ROW(
                        nestedRowType,
                        DataTypes.ARRAY(nestedRowType),
                        DataTypes.MAP(
                                nestedRowType.getTypeAt(0),
                                DataTypes.ARRAY(nestedRowType.getTypeAt(0))));
        BinaryRow row =
                new InternalRowSerializer(rowType)
                        .toBinaryRow(
                                GenericRow.of(
                                        GenericRow.of(vector),
                                        new GenericArray(new Object[] {GenericRow.of(vector)}),
                                        new GenericMap(
                                                Collections.singletonMap(
                                                        vector,
                                                        new GenericArray(new Object[] {vector})))));

        FlinkRowData rowData = new FlinkRowData(row, rowType);

        assertThat(rowData.getRow(0, 1).getArray(0).toFloatArray()).isEqualTo(values);
        assertThat(rowData.getArray(1).getRow(0, 1).getArray(0).toFloatArray()).isEqualTo(values);
        MapData map = rowData.getMap(2);
        assertThat(map.keyArray().getArray(0).toFloatArray()).isEqualTo(values);
        assertThat(map.valueArray().getArray(0).getArray(0).toFloatArray()).isEqualTo(values);
    }

    @Test
    public void testNullVector() {
        InternalRow row = GenericRow.of(1, null);
        FlinkRowData rowData = new FlinkRowData(row);

        assertThat(rowData.isNullAt(1)).isTrue();
    }
}
