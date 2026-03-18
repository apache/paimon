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

import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.VectorType;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link LogicalTypeConversion}. */
public class LogicalTypeConversionTest {

    @Test
    public void testToVectorType() {
        Map<String, String> options = new HashMap<>();
        options.put("vector-field", "v");
        options.put("field.v.vector-dim", "3");
        LogicalType flinkType = makeVectorLogicalType(new FloatType());
        VectorType vectorType = LogicalTypeConversion.toVectorType("v", flinkType, options);
        assertThat(vectorType).isEqualTo(DataTypes.VECTOR(3, DataTypes.FLOAT()));
    }

    @Test
    public void testToVectorTypeInvalidLogicalType() {
        Map<String, String> options = new HashMap<>();
        options.put("vector-field", "v");
        options.put("field.v.vector-dim", "3");
        assertThatThrownBy(() -> LogicalTypeConversion.toVectorType("v", new FloatType(), options))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> LogicalTypeConversion.toVectorType("v", new IntType(), options))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testToVectorTypeInvalidElementType() {
        Map<String, String> options = new HashMap<>();
        options.put("vector-field", "v");
        options.put("field.v.vector-dim", "3");
        LogicalType type1 = makeVectorLogicalType(new ArrayType(new FloatType()));
        assertThatThrownBy(() -> LogicalTypeConversion.toVectorType("v", type1, options))
                .isInstanceOf(IllegalArgumentException.class);
        LogicalType type2 = makeVectorLogicalType(new MapType(new IntType(), new FloatType()));
        assertThatThrownBy(() -> LogicalTypeConversion.toVectorType("v", type2, options))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testToVectorTypeNoDim() {
        Map<String, String> options = new HashMap<>();
        options.put("vector-field", "v");
        // options.put("field.v.vector-dim", "3");
        LogicalType flinkType = makeVectorLogicalType(new FloatType());
        assertThatThrownBy(() -> LogicalTypeConversion.toVectorType("v", flinkType, options))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testToVectorTypeInvalidDim() {
        Map<String, String> options = new HashMap<>();
        options.put("vector-field", "v");
        LogicalType flinkType = makeVectorLogicalType(new FloatType());
        options.put("field.v.vector-dim", "");
        assertThatThrownBy(() -> LogicalTypeConversion.toVectorType("v", flinkType, options))
                .isInstanceOf(IllegalArgumentException.class);
        options.put("field.v.vector-dim", "abc");
        assertThatThrownBy(() -> LogicalTypeConversion.toVectorType("v", flinkType, options))
                .isInstanceOf(IllegalArgumentException.class);
        options.put("field.v.vector-dim", "0");
        assertThatThrownBy(() -> LogicalTypeConversion.toVectorType("v", flinkType, options))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testVectorTypeToLogicalType() {
        LogicalType logicalType =
                LogicalTypeConversion.toLogicalType(DataTypes.VECTOR(4, DataTypes.FLOAT()));
        assertThat(logicalType).isInstanceOf(ArrayType.class);
        ArrayType arrayType = (ArrayType) logicalType;
        assertThat(arrayType.getElementType()).isInstanceOf(FloatType.class);
    }

    private LogicalType makeVectorLogicalType(LogicalType elementType) {
        return new ArrayType(elementType);
    }
}
