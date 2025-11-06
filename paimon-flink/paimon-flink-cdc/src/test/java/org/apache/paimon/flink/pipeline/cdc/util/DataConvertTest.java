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

package org.apache.paimon.flink.pipeline.cdc.util;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.InternalRowUtils;

import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Data convert test for {@link PaimonToFlinkCDCDataConverter} and {@link
 * FlinkCDCToPaimonDataConverter}.
 */
public class DataConvertTest {

    @Test
    void testFullTypesConversion() {
        Schema fullTypesSchema =
                Schema.newBuilder()
                        .physicalColumn("pk_string", DataTypes.STRING().notNull())
                        .physicalColumn("boolean", DataTypes.BOOLEAN())
                        .physicalColumn("binary", DataTypes.BINARY(3))
                        .physicalColumn("varbinary", DataTypes.VARBINARY(10))
                        .physicalColumn("bytes", DataTypes.BYTES())
                        .physicalColumn("tinyint", DataTypes.TINYINT())
                        .physicalColumn("smallint", DataTypes.SMALLINT())
                        .physicalColumn("int", DataTypes.INT())
                        .physicalColumn("bigint", DataTypes.BIGINT())
                        .physicalColumn("float", DataTypes.FLOAT())
                        .physicalColumn("double", DataTypes.DOUBLE())
                        .physicalColumn("decimal", DataTypes.DECIMAL(6, 3))
                        .physicalColumn("char", DataTypes.CHAR(5))
                        .physicalColumn("varchar", DataTypes.VARCHAR(10))
                        .physicalColumn("string", DataTypes.STRING())
                        .physicalColumn("date", DataTypes.DATE())
                        .physicalColumn("time", DataTypes.TIME())
                        .physicalColumn("time_with_precision", DataTypes.TIME(6))
                        .physicalColumn("timestamp", DataTypes.TIMESTAMP())
                        .physicalColumn("timestamp_with_precision_3", DataTypes.TIMESTAMP(3))
                        .physicalColumn("timestamp_with_precision_6", DataTypes.TIMESTAMP(6))
                        .physicalColumn("timestamp_with_precision_9", DataTypes.TIMESTAMP(9))
                        .physicalColumn("timestamp_ltz", DataTypes.TIMESTAMP_LTZ())
                        .physicalColumn(
                                "timestamp_ltz_with_precision_3", DataTypes.TIMESTAMP_LTZ(3))
                        .physicalColumn(
                                "timestamp_ltz_with_precision_6", DataTypes.TIMESTAMP_LTZ(6))
                        .physicalColumn(
                                "timestamp_ltz_with_precision_9", DataTypes.TIMESTAMP_LTZ(9))
                        .primaryKey("pk_string")
                        .partitionKey("boolean")
                        .build();

        Object[] testData =
                new Object[] {
                    BinaryStringData.fromString("pk_string"),
                    true,
                    new byte[] {1, 2, 3},
                    new byte[] {4, 5, 6},
                    new byte[] {7, 8, 9},
                    (byte) 1,
                    (short) 2,
                    3,
                    4L,
                    5.1f,
                    6.2,
                    DecimalData.fromBigDecimal(new BigDecimal("7.123"), 6, 3),
                    BinaryStringData.fromString("test1"),
                    BinaryStringData.fromString("test2"),
                    BinaryStringData.fromString("test3"),
                    DateData.fromEpochDay(1000),
                    TimeData.fromMillisOfDay(200),
                    TimeData.fromMillisOfDay(300),
                    TimestampData.fromMillis(100, 1),
                    TimestampData.fromMillis(200, 2),
                    TimestampData.fromMillis(300, 3),
                    TimestampData.fromMillis(400, 4),
                    LocalZonedTimestampData.fromEpochMillis(300, 3),
                    LocalZonedTimestampData.fromEpochMillis(400, 4),
                    LocalZonedTimestampData.fromEpochMillis(500, 5),
                    LocalZonedTimestampData.fromEpochMillis(600, 6),
                };

        Object[] expectedPaimonData = {
            BinaryString.fromString("pk_string"),
            true,
            new byte[] {1, 2, 3},
            new byte[] {4, 5, 6},
            new byte[] {7, 8, 9},
            (byte) 1,
            (short) 2,
            3,
            4L,
            5.1f,
            6.2,
            Decimal.fromBigDecimal(new BigDecimal("7.123"), 6, 3),
            BinaryString.fromString("test1"),
            BinaryString.fromString("test2"),
            BinaryString.fromString("test3"),
            1000,
            200,
            300,
            Timestamp.fromEpochMillis(100, 1),
            Timestamp.fromEpochMillis(200, 2),
            Timestamp.fromEpochMillis(300, 3),
            Timestamp.fromEpochMillis(400, 4),
            Timestamp.fromEpochMillis(300, 3),
            Timestamp.fromEpochMillis(400, 4),
            Timestamp.fromEpochMillis(500, 5),
            Timestamp.fromEpochMillis(600, 6),
        };

        testConvertBackAndForth(fullTypesSchema, testData, expectedPaimonData);

        Object[] allNullTestData =
                new Object[] {
                    BinaryStringData.fromString("pk_string"),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null
                };

        Object[] expectedAllNullPaimonData =
                new Object[] {
                    BinaryString.fromString("pk_string"),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null
                };
        testConvertBackAndForth(fullTypesSchema, allNullTestData, expectedAllNullPaimonData);
    }

    private void testConvertBackAndForth(
            Schema cdcSchema, Object[] fields, Object[] expectedInternalRows) {
        int arity = fields.length;
        TableId tableId = TableId.tableId("testDatabase", "testTable");
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(
                        cdcSchema.getColumnDataTypes().toArray(new DataType[0]));
        DataChangeEvent event =
                DataChangeEvent.insertEvent(tableId, recordDataGenerator.generate(fields));
        org.apache.paimon.schema.Schema paimonSchema =
                FlinkCDCToPaimonTypeConverter.convertFlinkCDCSchemaToPaimonSchema(cdcSchema);

        // Step 1: Convert CDC Event to Paimon Event
        List<RecordData.FieldGetter> cdcFieldGetters =
                FlinkCDCToPaimonDataConverter.createFieldGetters(cdcSchema.getColumnDataTypes());
        InternalRow paimonRow =
                FlinkCDCToPaimonDataConverter.convertDataChangeEventToInternalRow(
                        event, cdcFieldGetters);

        // Step 2: Check Paimon Row specs
        Assertions.assertThat(paimonRow.getRowKind()).isEqualTo(RowKind.INSERT);
        Assertions.assertThat(paimonRow.getFieldCount()).isEqualTo(arity);
        InternalRow.FieldGetter[] internalRowFieldGetters =
                InternalRowUtils.createFieldGetters(paimonSchema.rowType().getFieldTypes());

        List<Object> internalRowRepresentation = new ArrayList<>();
        for (InternalRow.FieldGetter internalRowFieldGetter : internalRowFieldGetters) {
            internalRowRepresentation.add(internalRowFieldGetter.getFieldOrNull(paimonRow));
        }
        Assertions.assertThat(internalRowRepresentation).containsExactly(expectedInternalRows);

        // Step 3: Convert it back
        List<InternalRow.FieldGetter> paimonFieldGetters =
                PaimonToFlinkCDCDataConverter.createFieldGetters(cdcSchema.getColumnDataTypes());
        DataChangeEvent convertedEvent =
                PaimonToFlinkCDCDataConverter.convertRowToDataChangeEvent(
                        tableId, paimonRow, paimonFieldGetters, recordDataGenerator);

        // Step 4: Validate it
        String[] originalFields = Arrays.stream(fields).map(this::stringify).toArray(String[]::new);
        Assertions.assertThat(convertedEvent).isEqualTo(event);
        Assertions.assertThat(
                        SchemaUtils.restoreOriginalData(
                                event.after(), SchemaUtils.createFieldGetters(cdcSchema)))
                .map(this::stringify)
                .containsExactly(originalFields);
    }

    // Stringify byte[] properly for checking.
    private String stringify(Object value) {
        if (value instanceof byte[]) {
            return Arrays.toString((byte[]) value);
        }
        return Objects.toString(value);
    }
}
