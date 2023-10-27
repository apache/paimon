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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.paimon.data.InternalRow.createFieldGetter;

/** Project {@link BinaryRow} fields into {@link InternalRow}. */
public class ProjectToRowFunction implements SerBiFunction<InternalRow, BinaryRow, InternalRow> {

    private final InternalRow.FieldGetter[] fieldGetters;

    private final Map<Integer, Integer> projectMapping;
    private final InternalRow.FieldGetter[] projectGetters;

    public ProjectToRowFunction(RowType rowType, List<String> projectFields) {
        DataType[] types = rowType.getFieldTypes().toArray(new DataType[0]);
        this.fieldGetters =
                IntStream.range(0, types.length)
                        .mapToObj(i -> createFieldGetter(types[i], i))
                        .toArray(InternalRow.FieldGetter[]::new);

        List<String> fieldNames = rowType.getFieldNames();
        this.projectMapping =
                projectFields.stream()
                        .collect(Collectors.toMap(fieldNames::indexOf, projectFields::indexOf));
        this.projectGetters =
                projectFields.stream()
                        .map(
                                field ->
                                        createFieldGetter(
                                                types[rowType.getFieldIndex(field)],
                                                projectFields.indexOf(field)))
                        .toArray(InternalRow.FieldGetter[]::new);
    }

    @Override
    public InternalRow apply(InternalRow input, BinaryRow project) {
        GenericRow newRow = new GenericRow(fieldGetters.length);
        for (int i = 0; i < fieldGetters.length; i++) {
            Object field =
                    projectMapping.containsKey(i)
                            ? projectGetters[projectMapping.get(i)].getFieldOrNull(project)
                            : fieldGetters[i].getFieldOrNull(input);
            newRow.setField(i, field);
        }
        return newRow;
    }
}
