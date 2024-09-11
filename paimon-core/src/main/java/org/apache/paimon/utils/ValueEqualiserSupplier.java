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

package org.apache.paimon.utils;

import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.apache.paimon.codegen.CodeGenUtils.newRecordEqualiser;

/** A {@link Supplier} that returns the equaliser for the file store value. */
public class ValueEqualiserSupplier implements SerializableSupplier<RecordEqualiser> {

    private static final long serialVersionUID = 1L;

    private final List<DataType> fieldTypes;

    private final int[] projection;

    public ValueEqualiserSupplier(RowType keyType) {
        this.fieldTypes = keyType.getFieldTypes();
        this.projection = null;
    }

    public ValueEqualiserSupplier(RowType keyType, int[] projection) {
        this.fieldTypes = keyType.getFieldTypes();
        this.projection = projection;
    }

    @Override
    public RecordEqualiser get() {
        return this.projection == null
                ? newRecordEqualiser(fieldTypes)
                : newRecordEqualiser(fieldTypes, projection);
    }

    public static ValueEqualiserSupplier fromIgnoreFields(
            RowType rowType, @Nullable List<String> ignoreFields) {
        int[] projection = getProjectionWithIgnoreFields(rowType, ignoreFields);
        return new ValueEqualiserSupplier(rowType, projection);
    }

    private static int[] getProjectionWithIgnoreFields(RowType rowType, List<String> ignoreFields) {
        List<String> fieldNames = rowType.getFieldNames();
        IntStream projectionStream = IntStream.range(0, rowType.getFieldCount());
        return projectionStream
                .filter(idx -> !ignoreFields.contains(fieldNames.get(idx)))
                .toArray();
    }
}
