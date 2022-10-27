/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.spark;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import org.apache.spark.sql.catalyst.CatalystTypeConverters;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Function1;

import static org.apache.flink.table.store.spark.SparkTypeTest.ALL_TYPES;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SparkInternalRow}. */
public class SparkInternalRowTest {

    @Test
    public void test() {
        Row row =
                Row.of(
                        1,
                        "jingsong",
                        22.2,
                        Stream.of(
                                        new AbstractMap.SimpleEntry<>("key1", Row.of(1.2, 2.3)),
                                        new AbstractMap.SimpleEntry<>("key2", Row.of(2.4, 3.5)))
                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
                        new String[] {"v1", "v5"},
                        new Integer[] {10, 30},
                        true,
                        (byte) 22,
                        (short) 356,
                        23567222L,
                        "varbinary_v".getBytes(StandardCharsets.UTF_8),
                        LocalDateTime.parse("2007-12-03T10:15:30"),
                        LocalDate.parse("2022-05-02"),
                        BigDecimal.valueOf(0.21),
                        BigDecimal.valueOf(65782123123.01),
                        BigDecimal.valueOf(62123123.5));

        RowRowConverter flinkConverter =
                RowRowConverter.create(TypeConversions.fromLogicalToDataType(ALL_TYPES));
        flinkConverter.open(Thread.currentThread().getContextClassLoader());
        RowData rowData = flinkConverter.toInternal(row);

        Function1<Object, Object> sparkConverter =
                CatalystTypeConverters.createToScalaConverter(
                        SparkTypeUtils.fromFlinkType(ALL_TYPES));
        org.apache.spark.sql.Row sparkRow =
                (org.apache.spark.sql.Row)
                        sparkConverter.apply(new SparkInternalRow(ALL_TYPES).replace(rowData));

        String expected =
                "{"
                        + "\"id\":1,"
                        + "\"name\":\"jingsong\","
                        + "\"salary\":22.2,"
                        + "\"locations\":{\"key1\":{\"posX\":1.2,\"posY\":2.3},\"key2\":{\"posX\":2.4,\"posY\":3.5}},"
                        + "\"strArray\":[\"v1\",\"v5\"],"
                        + "\"intArray\":[10,30],"
                        + "\"boolean\":true,"
                        + "\"tinyint\":22,"
                        + "\"smallint\":356,"
                        + "\"bigint\":23567222,"
                        + "\"bytes\":\"dmFyYmluYXJ5X3Y=\","
                        + "\"timestamp\":\"2007-12-03 10:15:30\","
                        + "\"date\":\"2022-05-02\","
                        + "\"decimal\":0.21,"
                        + "\"decimal2\":65782123123.01,"
                        + "\"decimal3\":62123123.5"
                        + "}";
        assertThat(sparkRow.json()).isEqualTo(expected);

        SparkRowData sparkRowData = new SparkRowData(ALL_TYPES, sparkRow);
        assertThat(flinkConverter.toExternal(sparkRowData)).isEqualTo(row);
    }
}
