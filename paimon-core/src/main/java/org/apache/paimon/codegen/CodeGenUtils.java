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

package org.apache.paimon.codegen;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import java.util.List;

/** Utils for code generations. */
public class CodeGenUtils {

    public static final Projection EMPTY_PROJECTION = input -> BinaryRow.EMPTY_ROW;

    public static Projection newProjection(RowType inputType, List<String> fields) {
        List<String> fieldNames = inputType.getFieldNames();
        int[] mapping = fields.stream().mapToInt(fieldNames::indexOf).toArray();
        return newProjection(inputType, mapping);
    }

    public static Projection newProjection(RowType inputType, int[] mapping) {
        if (mapping.length == 0) {
            return EMPTY_PROJECTION;
        }

        return CodeGenLoader.getCodeGenerator()
                .generateProjection("Projection", inputType, mapping)
                .newInstance(CodeGenUtils.class.getClassLoader());
    }

    public static NormalizedKeyComputer newNormalizedKeyComputer(
            List<DataType> fieldTypes, String name) {
        return CodeGenLoader.getCodeGenerator()
                .generateNormalizedKeyComputer(fieldTypes, name)
                .newInstance(CodeGenUtils.class.getClassLoader());
    }

    public static GeneratedClass<RecordComparator> generateRecordComparator(
            List<DataType> fieldTypes, String name) {
        return CodeGenLoader.getCodeGenerator().generateRecordComparator(fieldTypes, name);
    }

    public static GeneratedClass<RecordEqualiser> generateRecordEqualiser(
            List<DataType> fieldTypes, String name) {
        return CodeGenLoader.getCodeGenerator().generateRecordEqualiser(fieldTypes, name);
    }

    public static RecordComparator newRecordComparator(List<DataType> fieldTypes, String name) {
        return generateRecordComparator(fieldTypes, name)
                .newInstance(CodeGenUtils.class.getClassLoader());
    }
}
