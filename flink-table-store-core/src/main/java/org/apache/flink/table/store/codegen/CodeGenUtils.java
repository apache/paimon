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

package org.apache.flink.table.store.codegen;

import org.apache.flink.table.store.utils.BinaryRowDataUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

/** Utils for code generations. */
public class CodeGenUtils {

    public static final Projection EMPTY_PROJECTION = input -> BinaryRowDataUtil.EMPTY_ROW;

    public static Projection newProjection(RowType inputType, int[] mapping) {
        if (mapping.length == 0) {
            return EMPTY_PROJECTION;
        }

        return CodeGenLoader.getCodeGenerator()
                .generateProjection("Projection", inputType, mapping)
                .newInstance(CodeGenUtils.class.getClassLoader());
    }

    public static NormalizedKeyComputer newNormalizedKeyComputer(
            List<LogicalType> fieldTypes, String name) {
        return CodeGenLoader.getCodeGenerator()
                .generateNormalizedKeyComputer(fieldTypes, name)
                .newInstance(CodeGenUtils.class.getClassLoader());
    }

    public static GeneratedClass<RecordComparator> generateRecordComparator(
            List<LogicalType> fieldTypes, String name) {
        return CodeGenLoader.getCodeGenerator().generateRecordComparator(fieldTypes, name);
    }

    public static RecordComparator newRecordComparator(List<LogicalType> fieldTypes, String name) {
        return generateRecordComparator(fieldTypes, name)
                .newInstance(CodeGenUtils.class.getClassLoader());
    }
}
