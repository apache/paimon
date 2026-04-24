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

package org.apache.paimon.types;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.DataGetters;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/** Test for InternalRowToSizeVisitor. */
public class InternalRowToSizeVisitorTest {

    private List<BiFunction<DataGetters, Integer, Integer>> feildSizeCalculator;

    @BeforeEach
    void setUp() {
        RowType rowType =
                RowType.builder()
                        .field("a0", DataTypes.INT())
                        .field("a1", DataTypes.TINYINT())
                        .field("a2", DataTypes.SMALLINT())
                        .field("a3", DataTypes.BIGINT())
                        .field("a4", DataTypes.STRING())
                        .field("a5", DataTypes.DOUBLE())
                        .field("a6", DataTypes.ARRAY(DataTypes.STRING()))
                        .field("a7", DataTypes.CHAR(100))
                        .field("a8", DataTypes.VARCHAR(100))
                        .field("a9", DataTypes.BOOLEAN())
                        .field("a10", DataTypes.DATE())
                        .field("a11", DataTypes.TIME())
                        .field("a12", DataTypes.TIMESTAMP())
                        .field("a13", DataTypes.TIMESTAMP_MILLIS())
                        .field("a14", DataTypes.DECIMAL(3, 3))
                        .field("a15", DataTypes.BYTES())
                        .field("a16", DataTypes.FLOAT())
                        .field("a17", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                        .field("a18", DataTypes.ROW(DataTypes.FIELD(100, "b1", DataTypes.STRING())))
                        .field("a19", DataTypes.BINARY(100))
                        .field("a20", DataTypes.VARBINARY(100))
                        .field("a21", DataTypes.MULTISET(DataTypes.STRING()))
                        .field(
                                "a22",
                                DataTypes.ROW(
                                        DataTypes.FIELD(
                                                101,
                                                "b2",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                102,
                                                                "b3",
                                                                DataTypes.MAP(
                                                                        DataTypes.STRING(),
                                                                        DataTypes.STRING())),
                                                        DataTypes.FIELD(
                                                                103,
                                                                "b4",
                                                                DataTypes.ARRAY(
                                                                        DataTypes.STRING())),
                                                        DataTypes.FIELD(
                                                                104,
                                                                "b5",
                                                                DataTypes.MULTISET(
                                                                        DataTypes.STRING()))))))
                        .field("a23", DataTypes.MULTISET(DataTypes.STRING()))
                        .build();

        InternalRowToSizeVisitor internalRowToSizeVisitor = new InternalRowToSizeVisitor();
        feildSizeCalculator =
                rowType.getFieldTypes().stream()
                        .map(dataType -> dataType.accept(internalRowToSizeVisitor))
                        .collect(Collectors.toList());
    }

    @Test
    void testCalculatorSize() {
        GenericRow row = new GenericRow(24);

        row.setField(0, 1);
        Assertions.assertThat(feildSizeCalculator.get(0).apply(row, 0)).isEqualTo(4);

        row.setField(1, (byte) 1);
        Assertions.assertThat(feildSizeCalculator.get(1).apply(row, 1)).isEqualTo(1);

        row.setField(2, (short) 1);
        Assertions.assertThat(feildSizeCalculator.get(2).apply(row, 2)).isEqualTo(2);

        row.setField(3, 1L);
        Assertions.assertThat(feildSizeCalculator.get(3).apply(row, 3)).isEqualTo(8);

        row.setField(4, BinaryString.fromString("a"));
        Assertions.assertThat(feildSizeCalculator.get(4).apply(row, 4)).isEqualTo(1);

        row.setField(5, 0.5D);
        Assertions.assertThat(feildSizeCalculator.get(5).apply(row, 5)).isEqualTo(8);

        row.setField(6, new GenericArray(new Object[] {BinaryString.fromString("1")}));
        Assertions.assertThat(feildSizeCalculator.get(6).apply(row, 6)).isEqualTo(1);

        row.setField(7, BinaryString.fromString("3"));
        Assertions.assertThat(feildSizeCalculator.get(7).apply(row, 7)).isEqualTo(1);

        row.setField(8, BinaryString.fromString("3"));
        Assertions.assertThat(feildSizeCalculator.get(8).apply(row, 8)).isEqualTo(1);

        row.setField(9, true);
        Assertions.assertThat(feildSizeCalculator.get(9).apply(row, 9)).isEqualTo(1);

        row.setField(10, 375);
        Assertions.assertThat(feildSizeCalculator.get(10).apply(row, 10)).isEqualTo(4);

        row.setField(11, 100);
        Assertions.assertThat(feildSizeCalculator.get(11).apply(row, 11)).isEqualTo(4);

        row.setField(12, Timestamp.fromEpochMillis(1685548953000L));
        Assertions.assertThat(feildSizeCalculator.get(12).apply(row, 12)).isEqualTo(8);

        row.setField(13, Timestamp.fromEpochMillis(1685548953000L));
        Assertions.assertThat(feildSizeCalculator.get(13).apply(row, 13)).isEqualTo(8);

        row.setField(14, Decimal.fromBigDecimal(new BigDecimal("0.22"), 3, 3));
        Assertions.assertThat(feildSizeCalculator.get(14).apply(row, 14)).isEqualTo(2);

        row.setField(15, new byte[] {1, 5, 2});
        Assertions.assertThat(feildSizeCalculator.get(15).apply(row, 15)).isEqualTo(3);

        row.setField(16, 0.26F);
        Assertions.assertThat(feildSizeCalculator.get(16).apply(row, 16)).isEqualTo(4);

        row.setField(
                17,
                new GenericMap(
                        Collections.singletonMap(
                                BinaryString.fromString("k"), BinaryString.fromString("v"))));
        Assertions.assertThat(feildSizeCalculator.get(17).apply(row, 17)).isEqualTo(2);

        row.setField(18, GenericRow.of(BinaryString.fromString("cc")));
        Assertions.assertThat(feildSizeCalculator.get(18).apply(row, 18)).isEqualTo(2);

        row.setField(19, "bb".getBytes());
        Assertions.assertThat(feildSizeCalculator.get(19).apply(row, 19)).isEqualTo(2);

        row.setField(20, "aa".getBytes());
        Assertions.assertThat(feildSizeCalculator.get(20).apply(row, 20)).isEqualTo(2);

        row.setField(
                21, new GenericMap(Collections.singletonMap(BinaryString.fromString("set"), 1)));

        Assertions.assertThat(feildSizeCalculator.get(21).apply(row, 21)).isEqualTo(3);

        row.setField(
                22,
                GenericRow.of(
                        GenericRow.of(
                                new GenericMap(
                                        Collections.singletonMap(
                                                BinaryString.fromString("k"),
                                                BinaryString.fromString("v"))),
                                new GenericArray(new Object[] {BinaryString.fromString("1")}),
                                new GenericMap(
                                        Collections.singletonMap(
                                                BinaryString.fromString("set"), 1)))));
        Assertions.assertThat(feildSizeCalculator.get(22).apply(row, 22)).isEqualTo(6);

        Assertions.assertThat(feildSizeCalculator.get(23).apply(row, 23)).isEqualTo(0);
    }
}
