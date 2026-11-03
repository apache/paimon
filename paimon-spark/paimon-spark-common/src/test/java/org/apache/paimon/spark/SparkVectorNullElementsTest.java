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

package org.apache.paimon.spark;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;

import scala.collection.JavaConverters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests vector null-element validation for Spark V1 and V2 writes. */
class SparkVectorNullElementsTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRejectNullElements(boolean internal) {
        RowType type = RowType.of(DataTypes.VECTOR(3, DataTypes.FLOAT()));
        for (int nullIndex = 0; nullIndex < 3; nullIndex++) {
            Float[] values = new Float[] {1.0f, 2.0f, 3.0f};
            values[nullIndex] = null;
            InternalRow row = wrap(type, internal, values, false);
            assertThatThrownBy(() -> row.getVector(0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("VECTOR must not contain null elements")
                    .hasMessageContaining("element index " + nullIndex);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testValidAndNullVectors(boolean internal) {
        RowType type = RowType.of(DataTypes.VECTOR(3, DataTypes.FLOAT()));
        InternalRow row = wrap(type, internal, new Float[] {1.0f, 2.0f, 3.0f}, false);
        assertThat(row.getVector(0).toFloatArray()).containsExactly(1.0f, 2.0f, 3.0f);
        assertThat(wrap(type, internal, null, false).isNullAt(0)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testArrayStillAllowsNullElements(boolean internal) {
        RowType type = RowType.of(DataTypes.ARRAY(DataTypes.FLOAT()));
        InternalRow row = wrap(type, internal, new Float[] {1.0f, null, 3.0f}, false);
        assertThat(row.getArray(0).isNullAt(1)).isTrue();
    }

    @Test
    void testNestedVector() {
        RowType type = RowType.of(DataTypes.ARRAY(DataTypes.VECTOR(3, DataTypes.FLOAT())));
        InternalRow row = wrap(type, true, new Float[] {1.0f, null, 3.0f}, true);
        assertThatThrownBy(() -> row.getArray(0).getVector(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("VECTOR must not contain null elements");
    }

    private InternalRow wrap(RowType type, boolean internal, Float[] values, boolean nested) {
        if (internal) {
            Object array = values == null ? null : new GenericArrayData(values);
            if (nested) {
                array = new GenericArrayData(new Object[] {array});
            }
            return new SparkInternalRowWrapper((StructType) SparkTypeUtils.fromPaimonType(type), 1)
                    .replace(new GenericInternalRow(new Object[] {array}));
        }
        Object array =
                values == null ? null : JavaConverters.asScalaBuffer(Arrays.asList(values)).toSeq();
        if (nested) {
            array = JavaConverters.asScalaBuffer(Arrays.asList(array)).toSeq();
        }
        return new SparkRow(type, RowFactory.create(array));
    }
}
