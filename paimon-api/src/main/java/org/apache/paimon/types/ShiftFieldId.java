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

import java.util.List;
import java.util.stream.Collectors;

/**
 * Shift every field id in a type by a fixed offset. Unlike {@link ReassignFieldId} this preserves
 * the relative order and gaps of the existing ids, so the result is exactly the original id space
 * translated by {@code offset}.
 */
public class ShiftFieldId extends DataTypeDefaultVisitor<DataType> {

    private final int offset;

    public ShiftFieldId(int offset) {
        this.offset = offset;
    }

    public static DataType shift(DataType input, int offset) {
        return input.accept(new ShiftFieldId(offset));
    }

    @Override
    public DataType visit(ArrayType arrayType) {
        return new ArrayType(arrayType.isNullable(), arrayType.getElementType().accept(this));
    }

    @Override
    public DataType visit(VectorType vectorType) {
        return new VectorType(
                vectorType.isNullable(),
                vectorType.getLength(),
                vectorType.getElementType().accept(this));
    }

    @Override
    public DataType visit(MultisetType multisetType) {
        return new MultisetType(
                multisetType.isNullable(), multisetType.getElementType().accept(this));
    }

    @Override
    public DataType visit(MapType mapType) {
        return new MapType(
                mapType.isNullable(),
                mapType.getKeyType().accept(this),
                mapType.getValueType().accept(this));
    }

    @Override
    public DataType visit(RowType rowType) {
        List<DataField> fields =
                rowType.getFields().stream()
                        .map(
                                f ->
                                        new DataField(
                                                f.id() + offset,
                                                f.name(),
                                                f.type().accept(this),
                                                f.description(),
                                                f.defaultValue()))
                        .collect(Collectors.toList());
        return new RowType(rowType.isNullable(), fields);
    }

    @Override
    protected DataType defaultMethod(DataType dataType) {
        return dataType;
    }
}
