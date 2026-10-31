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

import org.apache.paimon.casting.DefaultValueRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Maps input fields to a full row and generates or validates physical generated columns. */
public final class GeneratedColumnRow {

    private final RowType rowType;
    private final int inputArity;
    private final int[] mapping;
    private final InternalRow.FieldGetter[] getters;
    private final List<GeneratedColumn> columns = new ArrayList<>();
    private final DefaultValueRow defaults;

    public GeneratedColumnRow(RowType rowType, RowType inputType) {
        this.rowType = rowType;
        this.inputArity = inputType.getFieldCount();
        this.mapping = new int[rowType.getFieldCount()];
        this.getters = new InternalRow.FieldGetter[rowType.getFieldCount()];
        List<DataField> inputFields = new ArrayList<>();
        for (DataField input : inputType.getFields()) {
            checkArgument(
                    rowType.containsField(input.name()),
                    "Unknown input column '%s'.",
                    input.name());
            DataField target = rowType.getField(input.name());
            checkArgument(
                    target.type().copy(true).equals(input.type().copy(true)),
                    "Input type for column '%s' does not match the table.",
                    input.name());
            inputFields.add(target);
        }
        this.defaults = DefaultValueRow.create(new RowType(inputFields));
        for (int i = 0; i < mapping.length; i++) {
            DataField field = rowType.getFields().get(i);
            mapping[i] = inputType.getFieldIndex(field.name());
            checkArgument(
                    mapping[i] >= 0 || field.generationExpression() != null,
                    "Missing input column '%s'. Only generated columns may be omitted.",
                    field.name());
            if (mapping[i] >= 0) {
                getters[i] =
                        InternalRowUtils.createNullCheckingFieldGetter(field.type(), mapping[i]);
            }
            if (field.generationExpression() != null) {
                columns.add(GeneratedColumn.create(rowType, field));
            }
        }
    }

    public InternalRow replaceRow(InternalRow input) {
        checkArgument(
                input.getFieldCount() == inputArity,
                "Input row has %s fields, expected %s.",
                input.getFieldCount(),
                inputArity);
        InternalRow source = defaults == null ? input : defaults.replaceRow(input);
        GenericRow result = new GenericRow(input.getRowKind(), rowType.getFieldCount());
        for (int i = 0; i < mapping.length; i++) {
            if (mapping[i] >= 0) {
                result.setField(i, getters[i].getFieldOrNull(source));
            }
        }
        for (GeneratedColumn column : columns) {
            int target = column.targetIndex();
            // Key-only deletes may carry the existing partition without its source value.
            if (input.getRowKind() == RowKind.DELETE && mapping[target] >= 0) {
                continue;
            }
            Integer expected = column.evaluate(result);
            if (mapping[target] >= 0) {
                checkArgument(
                        Objects.equals(result.getField(target), expected),
                        "Value for generated partition column '%s' does not match generation expression '%s'.",
                        rowType.getFields().get(target).name(),
                        rowType.getFields().get(target).generationExpression());
            }
            result.setField(target, expected);
        }
        return result;
    }

    public static boolean hasGeneratedColumns(RowType rowType) {
        return rowType.getFields().stream().anyMatch(f -> f.generationExpression() != null);
    }
}
