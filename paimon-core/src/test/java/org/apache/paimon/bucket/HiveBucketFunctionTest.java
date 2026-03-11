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

package org.apache.paimon.bucket;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link HiveBucketFunction}. */
class HiveBucketFunctionTest {

    @Test
    void testHiveBucketFunction() {
        RowType rowType =
                RowType.of(
                        DataTypes.INT(),
                        DataTypes.STRING(),
                        DataTypes.BYTES(),
                        DataTypes.DECIMAL(10, 4));
        HiveBucketFunction hiveBucketFunction = new HiveBucketFunction(rowType);

        BinaryRow row =
                toBinaryRow(
                        rowType,
                        7,
                        BinaryString.fromString("hello"),
                        new byte[] {1, 2, 3},
                        Decimal.fromBigDecimal(new BigDecimal("12.3400"), 10, 4));

        int expectedHash =
                31
                                * (31
                                                * (31 * 7
                                                        + HiveHasher.hashBytes(
                                                                "hello"
                                                                        .getBytes(
                                                                                StandardCharsets
                                                                                        .UTF_8)))
                                        + HiveHasher.hashBytes(new byte[] {1, 2, 3}))
                        + new BigDecimal("12.34").hashCode();
        assertThat(hiveBucketFunction.bucket(row, 8))
                .isEqualTo((expectedHash & Integer.MAX_VALUE) % 8);
    }

    @Test
    void testHiveBucketFunctionWithNulls() {
        RowType rowType = RowType.of(DataTypes.INT(), DataTypes.STRING());
        HiveBucketFunction hiveBucketFunction = new HiveBucketFunction(rowType);

        BinaryRow row = toBinaryRow(rowType, null, null);

        assertThat(hiveBucketFunction.bucket(row, 4)).isZero();
    }

    @Test
    void testHiveBucketFunctionUnsupportedType() {
        RowType rowType = RowType.of(DataTypes.TIMESTAMP());
        HiveBucketFunction hiveBucketFunction = new HiveBucketFunction(rowType);

        assertThat(hiveBucketFunction.bucket(toBinaryRow(rowType, (Object) null), 4)).isZero();

        assertThatThrownBy(
                        () ->
                                hiveBucketFunction.bucket(
                                        toBinaryRow(
                                                rowType,
                                                org.apache.paimon.data.Timestamp.fromEpochMillis(
                                                        1L)),
                                        4))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Unsupported type as bucket key type");
    }

    private BinaryRow toBinaryRow(RowType rowType, Object... values) {
        return new InternalRowSerializer(rowType).toBinaryRow(GenericRow.of(values));
    }
}
