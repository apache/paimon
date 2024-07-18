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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.utils.DateTimeUtils;

import org.apache.spark.sql.catalyst.CatalystTypeConverters;
import org.apache.spark.sql.catalyst.util.CharVarcharUtils;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.AbstractMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Function1;
import scala.collection.JavaConverters;

import static org.apache.paimon.data.BinaryString.fromString;
import static org.apache.paimon.spark.SparkTypeTest.ALL_TYPES;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SparkInternalRow}. */
public class SparkInternalRowTest {

    @Test
    public void test() {
        TimeZone tz = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
        InternalRow rowData =
                GenericRow.of(
                        1,
                        fromString("jingsong"),
                        fromString("apache"),
                        fromString("paimon"),
                        22.2,
                        new GenericMap(
                                Stream.of(
                                                new AbstractMap.SimpleEntry<>(
                                                        fromString("key1"),
                                                        GenericRow.of(1.2, 2.3)),
                                                new AbstractMap.SimpleEntry<>(
                                                        fromString("key2"),
                                                        GenericRow.of(2.4, 3.5)))
                                        .collect(
                                                Collectors.toMap(
                                                        Map.Entry::getKey, Map.Entry::getValue))),
                        new GenericArray(new BinaryString[] {fromString("v1"), fromString("v5")}),
                        new GenericArray(new Integer[] {10, 30}),
                        true,
                        (byte) 22,
                        (short) 356,
                        23567222L,
                        "varbinary_v".getBytes(StandardCharsets.UTF_8),
                        Timestamp.fromLocalDateTime(LocalDateTime.parse("2007-12-03T10:15:30")),
                        Timestamp.fromLocalDateTime(LocalDateTime.parse("2007-12-03T10:15:30")),
                        DateTimeUtils.toInternal(LocalDate.parse("2022-05-02")),
                        Decimal.fromBigDecimal(BigDecimal.valueOf(0.21), 2, 2),
                        Decimal.fromBigDecimal(BigDecimal.valueOf(65782123123.01), 38, 2),
                        Decimal.fromBigDecimal(BigDecimal.valueOf(62123123.5), 10, 1));

        // CatalystTypeConverters does not support char and varchar, we need to replace char and
        // varchar with string
        Function1<Object, Object> sparkConverter =
                CatalystTypeConverters.createToScalaConverter(
                        CharVarcharUtils.replaceCharVarcharWithString(
                                SparkTypeUtils.fromPaimonType(ALL_TYPES)));
        org.apache.spark.sql.Row sparkRow =
                (org.apache.spark.sql.Row)
                        sparkConverter.apply(new SparkInternalRow(ALL_TYPES).replace(rowData));

        String expected =
                "1,"
                        + "jingsong,"
                        + "apache,"
                        + "paimon,"
                        + "22.2,"
                        + "Map(key2 -> [2.4,3.5], key1 -> [1.2,2.3]),"
                        + "WrappedArray(v1, v5),"
                        + "WrappedArray(10, 30),"
                        + "true,"
                        + "22,"
                        + "356,"
                        + "23567222,"
                        + "[B@,"
                        + "2007-12-03 18:15:30.0,"
                        + "2007-12-03T10:15:30,"
                        + "2022-05-02,"
                        + "0.21,"
                        + "65782123123.01,"
                        + "62123123.5";
        assertThat(sparkRowToString(sparkRow)).isEqualTo(expected);

        SparkRow sparkRowData = new SparkRow(ALL_TYPES, sparkRow);
        sparkRow =
                (org.apache.spark.sql.Row)
                        sparkConverter.apply(new SparkInternalRow(ALL_TYPES).replace(sparkRowData));
        assertThat(sparkRowToString(sparkRow)).isEqualTo(expected);
        TimeZone.setDefault(tz);
    }

    private String sparkRowToString(org.apache.spark.sql.Row row) {
        return JavaConverters.seqAsJavaList(row.toSeq()).stream()
                .map(Object::toString)
                // Since the toString result of Spark's binary col is unstable, replace it
                .map(x -> x.startsWith("[B@") ? "[B@" : x)
                .collect(Collectors.joining(","));
    }
}
