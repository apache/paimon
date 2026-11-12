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

import org.apache.paimon.flink.action.cdc.kafka.KafkaMetadataConverter;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.cdc.ComputedColumnUtils.buildComputedColumns;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Test for ComputedColumnUtils. */
public class ComputedColumnUtilsTest {

    @Test
    public void testComputedColumns() {
        List<String> calColArgs =
                Arrays.asList(
                        "A=substring(B, 1)",
                        "B=substring(ExistedColumn,1)",
                        "C=now()",
                        "D=substring(A, 1)",
                        "E=substring(C,1)");
        List<DataField> physicalFields =
                Arrays.asList(new DataField(1, "ExistedColumn", DataTypes.STRING()));
        List<ComputedColumn> columns = buildComputedColumns(calColArgs, physicalFields);

        assertEquals(
                Arrays.asList("B", "C", "E", "A", "D"),
                columns.stream().map(ComputedColumn::columnName).collect(Collectors.toList()));
    }

    @Test
    public void testCaseInsensitiveKeepsArgumentsAsGiven() {
        List<DataField> physicalFields =
                Arrays.asList(
                        new DataField(0, "id", DataTypes.INT()),
                        new DataField(1, "create_time", DataTypes.TIMESTAMP(3)));
        List<ComputedColumn> columns =
                buildComputedColumns(
                        Arrays.asList(
                                "dt=date_format(CREATE_TIME,yyyy-MM-dd)",
                                "tag=cast(hello, STRING)"),
                        physicalFields,
                        false);

        assertEquals(
                Arrays.asList("dt", "tag"),
                columns.stream().map(ComputedColumn::columnName).collect(Collectors.toList()));
        // the pattern must not be case-converted: YYYY-MM-DD means week year and day of year
        assertEquals("2024-12-30", columns.get(0).eval("2024-12-30 10:00:00.000"));
        assertEquals("hello", columns.get(1).eval(null));
    }

    @Test
    public void testEvalFromRecordCaseInsensitive() {
        List<DataField> physicalFields =
                Arrays.asList(new DataField(0, "create_time", DataTypes.TIMESTAMP(3)));
        Map<String, String> record = new HashMap<>();
        // the source system keeps upper case names, the user wrote the reference in lower case
        record.put("CREATE_TIME", "2024-12-30 10:00:00.000");

        ComputedColumn caseInsensitive =
                buildComputedColumns(
                                Collections.singletonList("dt=date_format(create_time,yyyy-MM-dd)"),
                                physicalFields,
                                false)
                        .get(0);
        assertEquals("2024-12-30", caseInsensitive.evalFromRecord(record));

        ComputedColumn caseSensitive =
                buildComputedColumns(
                                Collections.singletonList("dt=date_format(create_time,yyyy-MM-dd)"),
                                physicalFields,
                                true)
                        .get(0);
        assertNull(caseSensitive.evalFromRecord(record));
        record.put("create_time", "2024-12-31 10:00:00.000");
        assertEquals("2024-12-31", caseSensitive.evalFromRecord(record));
        // an exact match wins over a match that ignores case
        assertEquals("2024-12-31", caseInsensitive.evalFromRecord(record));
        // a present key with a null value is a null input, not a reason to look at other columns
        record.put("create_time", null);
        assertNull(caseInsensitive.evalFromRecord(record));
    }

    @Test
    public void testCaseInsensitiveReferenceBetweenComputedColumns() {
        List<DataField> physicalFields = Arrays.asList(new DataField(0, "_date", DataTypes.DATE()));
        List<ComputedColumn> columns =
                buildComputedColumns(
                        Arrays.asList("_YEAR_STR=substring(_year, 0, 2)", "_year=year(_DATE)"),
                        physicalFields,
                        false);

        assertEquals(
                Arrays.asList("_year", "_YEAR_STR"),
                columns.stream().map(ComputedColumn::columnName).collect(Collectors.toList()));
        assertEquals(DataTypes.INT(), columns.get(0).columnType());
        assertEquals(DataTypes.STRING(), columns.get(1).columnType());
        assertEquals("20", columns.get(1).eval(columns.get(0).eval("2023-03-23")));
    }

    @Test
    public void testCaseInsensitiveMetadataReference() {
        CdcMetadataConverter timestamp =
                new PrefixedMetadataConverter(
                        new KafkaMetadataConverter.TimestampConverter(), "__kafka_");
        List<ComputedColumn> columns =
                buildComputedColumns(
                        Collections.singletonList("dt=date_format(__KAFKA_TIMESTAMP,yyyy-MM-dd)"),
                        Collections.emptyList(),
                        new CdcMetadataConverter[] {timestamp},
                        false);

        assertEquals(
                "2024-12-30",
                columns.get(0)
                        .evalFromRecord(
                                Collections.singletonMap(
                                        "__kafka_timestamp", "2024-12-30 10:00:00.000")));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testPhysicalFieldTypeTakesPrecedenceOverMetadata(boolean caseSensitive) {
        String sourceName = caseSensitive ? "offset" : "OFFSET";
        ComputedColumn column =
                buildComputedColumns(
                                Collections.singletonList("s=truncate(offset,2)"),
                                Collections.singletonList(
                                        new DataField(0, sourceName, DataTypes.STRING())),
                                new CdcMetadataConverter[] {
                                    new KafkaMetadataConverter.OffsetConverter()
                                },
                                caseSensitive)
                        .get(0);

        assertEquals(DataTypes.STRING(), column.columnType());
        assertEquals("so", column.eval("source"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testPhysicalFieldValueTakesPrecedenceOverMetadata(boolean caseSensitive) {
        String sourceName = caseSensitive ? "topic" : "TOPIC";
        ComputedColumn column =
                buildComputedColumns(
                                Collections.singletonList("s=substring(topic,0)"),
                                Collections.singletonList(
                                        new DataField(0, sourceName, DataTypes.STRING())),
                                caseSensitive)
                        .get(0);
        Map<String, String> source = new HashMap<>();
        source.put(sourceName, "source-value");
        Map<String, String> metadata = Collections.singletonMap("topic", "wire-topic");

        assertEquals(
                "source-value", column.evalFromRecord(source, Collections.emptySet(), metadata));
        source.put(sourceName, null);
        assertNull(column.evalFromRecord(source, Collections.emptySet(), metadata));
        source.clear();
        assertEquals(
                "wire-topic",
                column.evalFromRecord(source, Collections.singleton("topic"), metadata));
    }

    @Test
    public void testCycleReference() {
        List<String> calColArgs =
                Arrays.asList("A=substring(B, 1)", "B=substring(C, 1)", "C=substring(A, 1)");
        assertThrows(
                IllegalArgumentException.class,
                () -> buildComputedColumns(calColArgs, Collections.emptyList()));
    }
}
