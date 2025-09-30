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

import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

/**
 * Data convert test for {@link PaimonToFlinkCDCDataConverter} and {@link
 * FlinkCDCToPaimonDataConverter}.
 */
public class DataConvertTest {

    @Test
    public void testFullTypesConverter() {
        org.apache.flink.cdc.common.schema.Schema fullTypesSchema =
                org.apache.flink.cdc.common.schema.Schema.newBuilder()
                        .physicalColumn(
                                "pk_string",
                                org.apache.flink.cdc.common.types.DataTypes.STRING().notNull())
                        .physicalColumn(
                                "boolean", org.apache.flink.cdc.common.types.DataTypes.BOOLEAN())
                        .physicalColumn(
                                "binary", org.apache.flink.cdc.common.types.DataTypes.BINARY(3))
                        .physicalColumn(
                                "varbinary",
                                org.apache.flink.cdc.common.types.DataTypes.VARBINARY(10))
                        .physicalColumn(
                                "bytes", org.apache.flink.cdc.common.types.DataTypes.BYTES())
                        .physicalColumn(
                                "tinyint", org.apache.flink.cdc.common.types.DataTypes.TINYINT())
                        .physicalColumn(
                                "smallint", org.apache.flink.cdc.common.types.DataTypes.SMALLINT())
                        .physicalColumn("int", org.apache.flink.cdc.common.types.DataTypes.INT())
                        .physicalColumn(
                                "bigint", org.apache.flink.cdc.common.types.DataTypes.BIGINT())
                        .physicalColumn(
                                "float", org.apache.flink.cdc.common.types.DataTypes.FLOAT())
                        .physicalColumn(
                                "double", org.apache.flink.cdc.common.types.DataTypes.DOUBLE())
                        .physicalColumn(
                                "decimal",
                                org.apache.flink.cdc.common.types.DataTypes.DECIMAL(6, 3))
                        .physicalColumn("char", org.apache.flink.cdc.common.types.DataTypes.CHAR(5))
                        .physicalColumn(
                                "varchar", org.apache.flink.cdc.common.types.DataTypes.VARCHAR(10))
                        .physicalColumn(
                                "string", org.apache.flink.cdc.common.types.DataTypes.STRING())
                        .physicalColumn("date", org.apache.flink.cdc.common.types.DataTypes.DATE())
                        .physicalColumn("time", org.apache.flink.cdc.common.types.DataTypes.TIME())
                        .physicalColumn(
                                "time_with_precision",
                                org.apache.flink.cdc.common.types.DataTypes.TIME(6))
                        .physicalColumn(
                                "timestamp",
                                org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP())
                        .physicalColumn(
                                "timestamp_with_precision_3",
                                org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP(3))
                        .physicalColumn(
                                "timestamp_with_precision_6",
                                org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP(6))
                        .physicalColumn(
                                "timestamp_with_precision_9",
                                org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP(9))
                        .physicalColumn(
                                "timestamp_ltz",
                                org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP_LTZ())
                        .physicalColumn(
                                "timestamp_ltz_with_precision_3",
                                org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP_LTZ(3))
                        .physicalColumn(
                                "timestamp_ltz_with_precision_6",
                                org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP_LTZ(6))
                        .physicalColumn(
                                "timestamp_ltz_with_precision_9",
                                org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP_LTZ(9))
                        .primaryKey("pk_string")
                        .partitionKey("boolean")
                        .build();
        TableId tableId = TableId.tableId("testDatabase", "testTable");
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(
                        fullTypesSchema.getColumnDataTypes().toArray(new DataType[0]));
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
                    TimeData.fromMillisOfDay(300).toMillisOfDay(),
                    TimestampData.fromMillis(100, 1),
                    TimestampData.fromMillis(200, 2),
                    TimestampData.fromMillis(300, 3),
                    TimestampData.fromMillis(400, 4),
                    LocalZonedTimestampData.fromEpochMillis(300, 3),
                    LocalZonedTimestampData.fromEpochMillis(400, 4),
                    LocalZonedTimestampData.fromEpochMillis(500, 5),
                    LocalZonedTimestampData.fromEpochMillis(600, 6),
                };
        org.apache.flink.cdc.common.event.DataChangeEvent dataChangeEvent =
                DataChangeEvent.insertEvent(tableId, recordDataGenerator.generate(testData));

        Assertions.assertEquals(
                dataChangeEvent,
                PaimonToFlinkCDCDataConverter.convertRowToDataChangeEvent(
                        tableId,
                        FlinkCDCToPaimonDataConverter.convertDataChangeEventToInternalRow(
                                dataChangeEvent,
                                FlinkCDCToPaimonDataConverter.createFieldGetters(
                                        fullTypesSchema.getColumnDataTypes())),
                        PaimonToFlinkCDCDataConverter.createFieldGetters(
                                fullTypesSchema.getColumnDataTypes()),
                        recordDataGenerator));
    }
}
