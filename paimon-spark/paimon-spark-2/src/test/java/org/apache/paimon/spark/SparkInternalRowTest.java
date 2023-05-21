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
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Function1;

import static org.apache.paimon.data.BinaryString.fromString;
import static org.apache.paimon.spark.SparkTypeTest.ALL_TYPES;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SparkInternalRow}. */
public class SparkInternalRowTest {

    @Test
    public void test() {
        InternalRow rowData =
                GenericRow.of(
                        1,
                        fromString("jingsong"),
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
                        DateTimeUtils.toInternal(LocalDate.parse("2022-05-02")),
                        Decimal.fromBigDecimal(BigDecimal.valueOf(0.21), 2, 2),
                        Decimal.fromBigDecimal(BigDecimal.valueOf(65782123123.01), 38, 2),
                        Decimal.fromBigDecimal(BigDecimal.valueOf(62123123.5), 10, 1));

        Function1<Object, Object> sparkConverter =
                CatalystTypeConverters.createToScalaConverter(
                        SparkTypeUtils.fromPaimonType(ALL_TYPES));
        org.apache.spark.sql.Row sparkRow =
                (org.apache.spark.sql.Row)
                        sparkConverter.apply(new SparkInternalRow(ALL_TYPES).replace(rowData));

        assertThat(sparkRow.getInt(0)).isEqualTo(1);
        assertThat(sparkRow.getString(1)).isEqualTo("jingsong");
        assertThat(sparkRow.getDouble(2)).isEqualTo(22.2);
        assertThat(((GenericRowWithSchema) sparkRow.getJavaMap(3).get("key1")).values())
                .isEqualTo(new Object[] {1.2, 2.3});
        assertThat(((GenericRowWithSchema) sparkRow.getJavaMap(3).get("key2")).values())
                .isEqualTo(new Object[] {2.4, 3.5});
        assertThat(sparkRow.getList(4)).isEqualTo(Arrays.asList("v1", "v5"));
        assertThat(sparkRow.getList(5)).isEqualTo(Arrays.asList(10, 30));
        assertThat(sparkRow.getBoolean(6)).isEqualTo(true);
        assertThat(sparkRow.getByte(7)).isEqualTo((byte) 22);
        assertThat(sparkRow.getShort(8)).isEqualTo((short) 356);
        assertThat(sparkRow.getLong(9)).isEqualTo(23567222L);
        assertThat((byte[]) sparkRow.get(10))
                .isEqualTo("varbinary_v".getBytes(StandardCharsets.UTF_8));
        assertThat(sparkRow.getTimestamp(11).getTime())
                .isEqualTo(
                        java.sql.Timestamp.valueOf(LocalDateTime.parse("2007-12-03T10:15:30"))
                                .getTime());
        assertThat(sparkRow.getDate(12).getTime())
                .isEqualTo(Date.valueOf(LocalDate.parse("2022-05-02")).getTime());
        assertThat(sparkRow.getDecimal(13)).isEqualTo(BigDecimal.valueOf(0.21));
        assertThat(sparkRow.getDecimal(14)).isEqualTo(BigDecimal.valueOf(65782123123.01));
        assertThat(sparkRow.getDecimal(15)).isEqualTo(BigDecimal.valueOf(62123123.5));
    }
}
