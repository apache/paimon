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

package org.apache.paimon.flink.action.cdc;

import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarCharType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link Expression.TruncateComputer}. */
public class TruncateComputerTest {

    private static Object[][] prepareData() {
        return new Object[][] {
            {"computedColumnField", "0", new TinyIntType(true), "10", "0"},
            {"computedColumnField", "1", new TinyIntType(true), "10", "0"},
            {"computedColumnField", "5", new TinyIntType(true), "10", "0"},
            {"computedColumnField", "9", new TinyIntType(true), "10", "0"},
            {"computedColumnField", "10", new TinyIntType(true), "10", "10"},
            {"computedColumnField", "11", new TinyIntType(true), "10", "10"},
            {"computedColumnField", "15", new TinyIntType(true), "10", "10"},
            {"computedColumnField", "-1", new TinyIntType(true), "10", "-10"},
            {"computedColumnField", "-5", new TinyIntType(true), "10", "-10"},
            {"computedColumnField", "-9", new TinyIntType(true), "10", "-10"},
            {"computedColumnField", "-10", new TinyIntType(true), "10", "-10"},
            {"computedColumnField", "-11", new TinyIntType(true), "10", "-20"},
            {"computedColumnField", "0", new SmallIntType(true), "10", "0"},
            {"computedColumnField", "1", new SmallIntType(true), "10", "0"},
            {"computedColumnField", "5", new SmallIntType(true), "10", "0"},
            {"computedColumnField", "9", new SmallIntType(true), "10", "0"},
            {"computedColumnField", "10", new SmallIntType(true), "10", "10"},
            {"computedColumnField", "11", new SmallIntType(true), "10", "10"},
            {"computedColumnField", "15", new SmallIntType(true), "10", "10"},
            {"computedColumnField", "-1", new SmallIntType(true), "10", "-10"},
            {"computedColumnField", "-5", new SmallIntType(true), "10", "-10"},
            {"computedColumnField", "-9", new SmallIntType(true), "10", "-10"},
            {"computedColumnField", "-10", new SmallIntType(true), "10", "-10"},
            {"computedColumnField", "-11", new SmallIntType(true), "10", "-20"},
            {"computedColumnField", "0", new IntType(true), "10", "0"},
            {"computedColumnField", "1", new IntType(true), "10", "0"},
            {"computedColumnField", "5", new IntType(true), "10", "0"},
            {"computedColumnField", "9", new IntType(true), "10", "0"},
            {"computedColumnField", "10", new IntType(true), "10", "10"},
            {"computedColumnField", "11", new IntType(true), "10", "10"},
            {"computedColumnField", "15", new IntType(true), "10", "10"},
            {"computedColumnField", "-1", new IntType(true), "10", "-10"},
            {"computedColumnField", "-5", new IntType(true), "10", "-10"},
            {"computedColumnField", "-9", new IntType(true), "10", "-10"},
            {"computedColumnField", "-10", new IntType(true), "10", "-10"},
            {"computedColumnField", "-11", new IntType(true), "10", "-20"},
            {"computedColumnField", "0", new BigIntType(true), "10", "0"},
            {"computedColumnField", "1", new BigIntType(true), "10", "0"},
            {"computedColumnField", "5", new BigIntType(true), "10", "0"},
            {"computedColumnField", "9", new BigIntType(true), "10", "0"},
            {"computedColumnField", "10", new BigIntType(true), "10", "10"},
            {"computedColumnField", "11", new BigIntType(true), "10", "10"},
            {"computedColumnField", "15", new BigIntType(true), "10", "10"},
            {"computedColumnField", "-1", new BigIntType(true), "10", "-10"},
            {"computedColumnField", "-5", new BigIntType(true), "10", "-10"},
            {"computedColumnField", "-9", new BigIntType(true), "10", "-10"},
            {"computedColumnField", "-10", new BigIntType(true), "10", "-10"},
            {"computedColumnField", "-11", new BigIntType(true), "10", "-20"},
            {"computedColumnField", "12.34", new DecimalType(9, 2), "10", "12.30"},
            {"computedColumnField", "12.30", new DecimalType(9, 2), "10", "12.30"},
            {"computedColumnField", "12.29", new DecimalType(9, 2), "10", "12.20"},
            {"computedColumnField", "0.05", new DecimalType(9, 2), "10", "0.00"},
            {"computedColumnField", "-0.05", new DecimalType(9, 2), "10", "-0.10"},
            {"computedColumnField", "abcde", new VarCharType(true, 5), "3", "abc"},
            {"computedColumnField", "abcdefg", new VarCharType(true, 7), "3", "abc"},
            {"computedColumnField", "abcdefg", new VarCharType(true, 7), "5", "abcde"},
            {"computedColumnField", "abcdefg", new VarCharType(true, 7), "7", "abcdefg"},
            {"computedColumnField", "abcde", new CharType(true, 5), "3", "abc"},
            {"computedColumnField", "abcdefg", new CharType(true, 7), "3", "abc"},
            {"computedColumnField", "abcdefg", new CharType(true, 7), "5", "abcde"},
            {"computedColumnField", "abcdefg", new CharType(true, 7), "7", "abcdefg"},
        };
    }

    @Test
    public void testTruncate() {
        Object[][] testData = prepareData();
        for (int i = 0; i < testData.length; i++) {
            String field = (String) testData[i][0];
            String value = (String) testData[i][1];
            DataType dataType = (DataType) testData[i][2];
            String literal = (String) testData[i][3];
            String expected = (String) testData[i][4];

            Expression.TruncateComputer truncateComputer =
                    new Expression.TruncateComputer(field, dataType, literal);
            assertThat(truncateComputer.eval(value)).isEqualTo(expected);
        }
    }

    @Test
    public void testTruncateWithException() {
        String fieldReference = "computedColumnField";
        DataType dataType = new CharType(true, 5);
        Expression.TruncateComputer truncateComputer =
                new Expression.TruncateComputer(fieldReference, dataType, "7");

        assertThatThrownBy(() -> truncateComputer.eval("abcde"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Invalid width value for truncate function: 7, expected less than or equal to 5.");

        DataType notSupportedDataType = new BooleanType();
        Expression.TruncateComputer notSupportTruncateComputer =
                new Expression.TruncateComputer(fieldReference, notSupportedDataType, "7");
        assertThatThrownBy(() -> notSupportTruncateComputer.eval("true"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported field type for truncate function: BOOLEAN");
    }
}
