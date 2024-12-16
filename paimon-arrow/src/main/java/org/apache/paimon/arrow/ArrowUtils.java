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

package org.apache.paimon.arrow;

import org.apache.paimon.arrow.vector.ArrowCStruct;
import org.apache.paimon.arrow.writer.ArrowFieldWriter;
import org.apache.paimon.arrow.writer.ArrowFieldWriterFactoryVisitor;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** Utilities for creating Arrow objects. */
public class ArrowUtils {

    static final String PARQUET_FIELD_ID = "PARQUET:field_id";

    public static VectorSchemaRoot createVectorSchemaRoot(
            RowType rowType, BufferAllocator allocator) {
        return createVectorSchemaRoot(rowType, allocator, true);
    }

    public static VectorSchemaRoot createVectorSchemaRoot(
            RowType rowType, BufferAllocator allocator, boolean caseSensitive) {
        List<Field> fields =
                rowType.getFields().stream()
                        .map(
                                f ->
                                        toArrowField(
                                                caseSensitive ? f.name() : f.name().toLowerCase(),
                                                f.id(),
                                                f.type(),
                                                0))
                        .collect(Collectors.toList());
        return VectorSchemaRoot.create(new Schema(fields), allocator);
    }

    public static FieldVector createVector(
            DataField dataField, BufferAllocator allocator, boolean caseSensitive) {
        return toArrowField(
                        caseSensitive ? dataField.name() : dataField.name().toLowerCase(),
                        dataField.id(),
                        dataField.type(),
                        0)
                .createVector(allocator);
    }

    public static Field toArrowField(String fieldName, int fieldId, DataType dataType, int depth) {
        FieldType fieldType = dataType.accept(ArrowFieldTypeConversion.ARROW_FIELD_TYPE_VISITOR);
        fieldType =
                new FieldType(
                        fieldType.isNullable(),
                        fieldType.getType(),
                        fieldType.getDictionary(),
                        Collections.singletonMap(PARQUET_FIELD_ID, String.valueOf(fieldId)));
        List<Field> children = null;
        if (dataType instanceof ArrayType) {
            Field field =
                    toArrowField(
                            ListVector.DATA_VECTOR_NAME,
                            fieldId,
                            ((ArrayType) dataType).getElementType(),
                            depth + 1);
            FieldType typeInner = field.getFieldType();
            field =
                    new Field(
                            field.getName(),
                            new FieldType(
                                    typeInner.isNullable(),
                                    typeInner.getType(),
                                    typeInner.getDictionary(),
                                    Collections.singletonMap(
                                            PARQUET_FIELD_ID,
                                            String.valueOf(
                                                    SpecialFields.getArrayElementFieldId(
                                                            fieldId, depth + 1)))),
                            field.getChildren());
            children = Collections.singletonList(field);
        } else if (dataType instanceof MapType) {
            MapType mapType = (MapType) dataType;

            Field keyField =
                    toArrowField(
                            MapVector.KEY_NAME, fieldId, mapType.getKeyType().notNull(), depth + 1);
            FieldType keyType = keyField.getFieldType();
            keyField =
                    new Field(
                            keyField.getName(),
                            new FieldType(
                                    keyType.isNullable(),
                                    keyType.getType(),
                                    keyType.getDictionary(),
                                    Collections.singletonMap(
                                            PARQUET_FIELD_ID,
                                            String.valueOf(
                                                    SpecialFields.getMapKeyFieldId(
                                                            fieldId, depth + 1)))),
                            keyField.getChildren());

            Field valueField =
                    toArrowField(
                            MapVector.VALUE_NAME,
                            fieldId,
                            mapType.getValueType().notNull(),
                            depth + 1);
            FieldType valueType = valueField.getFieldType();
            valueField =
                    new Field(
                            valueField.getName(),
                            new FieldType(
                                    valueType.isNullable(),
                                    valueType.getType(),
                                    valueType.getDictionary(),
                                    Collections.singletonMap(
                                            PARQUET_FIELD_ID,
                                            String.valueOf(
                                                    SpecialFields.getMapValueFieldId(
                                                            fieldId, depth + 1)))),
                            valueField.getChildren());

            FieldType structType =
                    new FieldType(
                            false,
                            Types.MinorType.STRUCT.getType(),
                            null,
                            Collections.singletonMap(PARQUET_FIELD_ID, String.valueOf(fieldId)));
            Field mapField =
                    new Field(
                            MapVector.DATA_VECTOR_NAME,
                            // data vector, key vector and value vector CANNOT be null
                            structType,
                            Arrays.asList(keyField, valueField));

            children = Collections.singletonList(mapField);
        } else if (dataType instanceof RowType) {
            RowType rowType = (RowType) dataType;
            children = new ArrayList<>();
            for (DataField field : rowType.getFields()) {
                children.add(toArrowField(field.name(), field.id(), field.type(), 0));
            }
        }
        return new Field(fieldName, fieldType, children);
    }

    public static ArrowFieldWriter[] createArrowFieldWriters(
            VectorSchemaRoot vectorSchemaRoot, RowType rowType) {
        ArrowFieldWriter[] fieldWriters = new ArrowFieldWriter[rowType.getFieldCount()];
        List<FieldVector> vectors = vectorSchemaRoot.getFieldVectors();

        for (int i = 0; i < rowType.getFieldCount(); i++) {
            fieldWriters[i] =
                    rowType.getTypeAt(i)
                            .accept(ArrowFieldWriterFactoryVisitor.INSTANCE)
                            .create(vectors.get(i));
        }

        return fieldWriters;
    }

    public static long timestampToEpoch(
            Timestamp timestamp, int precision, @Nullable ZoneId castZoneId) {
        return castZoneId == null
                ? nonCastedTimestampToEpoch(timestamp, precision)
                : zoneCastedTimestampZoneCastToEpoch(timestamp, precision, castZoneId);
    }

    public static ArrowCStruct serializeToCStruct(
            VectorSchemaRoot vsr, ArrowArray array, ArrowSchema schema) {
        BufferAllocator bufferAllocator = vsr.getVector(0).getAllocator();
        Data.exportVectorSchemaRoot(bufferAllocator, vsr, null, array, schema);
        return ArrowCStruct.of(array, schema);
    }

    public static void serializeToIpc(VectorSchemaRoot vsr, OutputStream out) {
        try (ArrowStreamWriter writer = new ArrowStreamWriter(vsr, null, out)) {
            writer.start();
            writer.writeBatch();
            writer.end();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize VectorSchemaRoot to IPC", e);
        }
    }

    private static long nonCastedTimestampToEpoch(Timestamp timestamp, int precision) {
        if (precision == 0) {
            return timestamp.getMillisecond() / 1000;
        } else if (precision >= 1 && precision <= 3) {
            return timestamp.getMillisecond();
        } else if (precision >= 4 && precision <= 6) {
            return timestamp.toMicros();
        } else {
            return timestamp.getMillisecond() * 1_000_000 + timestamp.getNanoOfMillisecond();
        }
    }

    private static long zoneCastedTimestampZoneCastToEpoch(
            Timestamp timestamp, int precision, ZoneId castZoneId) {
        Instant instant = timestamp.toLocalDateTime().atZone(castZoneId).toInstant();
        if (precision == 0) {
            return instant.getEpochSecond();
        } else if (precision >= 1 && precision <= 3) {
            return instant.getEpochSecond() * 1_000 + instant.getNano() / 1_000_000;
        } else if (precision >= 4 && precision <= 6) {
            return instant.getEpochSecond() * 1_000_000 + instant.getNano() / 1_000;
        } else {
            return instant.getEpochSecond() * 1_000_000_000 + instant.getNano();
        }
    }
}
