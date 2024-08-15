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

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** Utilities for creating Arrow objects. */
public class ArrowUtils {

    public static VectorSchemaRoot createVectorSchemaRoot(
            RowType rowType, BufferAllocator allocator) {
        return createVectorSchemaRoot(rowType, allocator, true);
    }

    public static VectorSchemaRoot createVectorSchemaRoot(
            RowType rowType, BufferAllocator allocator, boolean allowUpperCase) {
        List<Field> fields =
                rowType.getFields().stream()
                        .map(
                                f ->
                                        toArrowField(
                                                allowUpperCase ? f.name() : f.name().toLowerCase(),
                                                f.type()))
                        .collect(Collectors.toList());
        return VectorSchemaRoot.create(new Schema(fields), allocator);
    }

    private static Field toArrowField(String fieldName, DataType dataType) {
        FieldType fieldType = dataType.accept(ToFieldTypeVisitor.INSTANCE);
        List<Field> children = null;
        if (dataType instanceof ArrayType) {
            children =
                    Collections.singletonList(
                            toArrowField(
                                    ListVector.DATA_VECTOR_NAME,
                                    ((ArrayType) dataType).getElementType()));
        } else if (dataType instanceof MapType) {
            MapType mapType = (MapType) dataType;
            children =
                    Collections.singletonList(
                            new Field(
                                    MapVector.DATA_VECTOR_NAME,
                                    // data vector, key vector and value vector CANNOT be null
                                    new FieldType(false, Types.MinorType.STRUCT.getType(), null),
                                    Arrays.asList(
                                            toArrowField(
                                                    MapVector.KEY_NAME,
                                                    mapType.getKeyType().notNull()),
                                            toArrowField(
                                                    MapVector.VALUE_NAME,
                                                    mapType.getValueType().notNull()))));
        } else if (dataType instanceof RowType) {
            RowType rowType = (RowType) dataType;
            children =
                    rowType.getFields().stream()
                            .map(f -> toArrowField(f.name(), f.type()))
                            .collect(Collectors.toList());
        }
        return new Field(fieldName, fieldType, children);
    }
}
