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
import org.apache.flink.table.types.logical.LogicalType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link LogicalTypeConversion}. */
public class LogicalTypeConversionTest {

    @Test
    public void testToVectorType() {
        VectorType vectorType = LogicalTypeConversion.toVectorType(new FloatType(), "3");
        assertThat(vectorType).isEqualTo(DataTypes.VECTOR(3, DataTypes.FLOAT()));
    }

    @Test
    public void testToVectorTypeInvalidDim() {
        assertThatThrownBy(() -> LogicalTypeConversion.toVectorType(new FloatType(), ""))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> LogicalTypeConversion.toVectorType(new FloatType(), "abc"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> LogicalTypeConversion.toVectorType(new FloatType(), "0"))
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
}
