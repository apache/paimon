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

import org.apache.paimon.data.InternalRow;

import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeChecks;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/** Converter from Paimon to Flink CDC data. */
public class PaimonToFlinkCDCDataConverter {

    /** Convert Paimon row to Flink CDC data. */
    public static DataChangeEvent convertRowToDataChangeEvent(
            TableId tableId,
            InternalRow row,
            List<InternalRow.FieldGetter> fieldGetters,
            BinaryRecordDataGenerator recordDataGenerator) {
        Object[] objects = new Object[row.getFieldCount()];
        for (int i = 0; i < row.getFieldCount(); i++) {
            objects[i] = fieldGetters.get(i).getFieldOrNull(row);
        }
        BinaryRecordData binaryRecordData = recordDataGenerator.generate(objects);
        switch (row.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                {
                    return DataChangeEvent.insertEvent(tableId, binaryRecordData, new HashMap<>());
                }
            case DELETE:
            case UPDATE_BEFORE:
                {
                    return DataChangeEvent.deleteEvent(tableId, binaryRecordData, new HashMap<>());
                }
            default:
                throw new IllegalArgumentException("Unsupported RowKind type: " + row.getRowKind());
        }
    }

    public static List<InternalRow.FieldGetter> createFieldGetters(List<DataType> fieldTypes) {
        List<InternalRow.FieldGetter> fieldGetters = new ArrayList<>();
        for (int i = 0; i < fieldTypes.size(); i++) {
            fieldGetters.add(createFieldGetter(fieldTypes.get(i), i));
        }
        return fieldGetters;
    }

    public static InternalRow.FieldGetter createFieldGetter(DataType fieldType, int fieldPos) {
        final InternalRow.FieldGetter fieldGetter;
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                fieldGetter =
                        row -> BinaryStringData.fromString(row.getString(fieldPos).toString());
                break;
            case BOOLEAN:
                fieldGetter = row -> row.getBoolean(fieldPos);
                break;
            case BINARY:
            case VARBINARY:
                fieldGetter = row -> row.getBinary(fieldPos);
                break;
            case DECIMAL:
                final int decimalPrecision = DataTypeChecks.getPrecision(fieldType);
                final int decimalScale = DataTypeChecks.getScale(fieldType);
                fieldGetter =
                        row ->
                                DecimalData.fromBigDecimal(
                                        row.getDecimal(fieldPos, decimalPrecision, decimalScale)
                                                .toBigDecimal(),
                                        decimalPrecision,
                                        decimalScale);
                break;
            case TINYINT:
                fieldGetter = row -> row.getByte(fieldPos);
                break;
            case SMALLINT:
                fieldGetter = row -> row.getShort(fieldPos);
                break;
            case BIGINT:
                fieldGetter = row -> row.getLong(fieldPos);
                break;
            case FLOAT:
                fieldGetter = row -> row.getFloat(fieldPos);
                break;
            case DOUBLE:
                fieldGetter = row -> row.getDouble(fieldPos);
                break;
            case INTEGER:
                fieldGetter = row -> row.getInt(fieldPos);
                break;
            case DATE:
                fieldGetter = row -> DateData.fromEpochDay(row.getInt(fieldPos));
                break;
            case TIME_WITHOUT_TIME_ZONE:
                fieldGetter = row -> TimeData.fromMillisOfDay(row.getInt(fieldPos));
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                fieldGetter =
                        row ->
                                TimestampData.fromTimestamp(
                                        row.getTimestamp(
                                                        fieldPos,
                                                        DataTypeChecks.getPrecision(fieldType))
                                                .toSQLTimestamp());
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                fieldGetter =
                        row ->
                                LocalZonedTimestampData.fromInstant(
                                        row.getTimestamp(
                                                        fieldPos,
                                                        DataTypeChecks.getPrecision(fieldType))
                                                .toInstant());
                break;
            default:
                throw new IllegalArgumentException("Unsupported type: " + fieldType.getTypeRoot());
        }
        if (!fieldType.isNullable()) {
            return fieldGetter;
        }
        return row -> {
            if (row.isNullAt(fieldPos)) {
                return null;
            }
            return fieldGetter.getFieldOrNull(row);
        };
    }
}
