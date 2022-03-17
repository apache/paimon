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

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.planner.codegen.sort.ComparatorCodeGenerator;
import org.apache.flink.table.planner.codegen.sort.SortCodeGenerator;
import org.apache.flink.table.planner.plan.utils.SortUtil;
import org.apache.flink.table.runtime.generated.GeneratedNormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.store.utils.TypeUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.stream.IntStream;

/** Default implementation of {@link CodeGenerator}. */
public class CodeGeneratorImpl implements CodeGenerator {

    @Override
    public GeneratedProjection generateProjection(
            TableConfig tableConfig, String name, RowType inputType, int[] inputMapping) {
        RowType outputType = TypeUtils.project(inputType, inputMapping);
        return ProjectionCodeGenerator.generateProjection(
                CodeGeneratorContext.apply(tableConfig), name, inputType, outputType, inputMapping);
    }

    @Override
    public GeneratedNormalizedKeyComputer generateNormalizedKeyComputer(
            TableConfig tableConfig, List<LogicalType> fieldTypes, String name) {
        return new SortCodeGenerator(
                        tableConfig,
                        RowType.of(fieldTypes.toArray(new LogicalType[0])),
                        SortUtil.getAscendingSortSpec(
                                IntStream.range(0, fieldTypes.size()).toArray()))
                .generateNormalizedKeyComputer(name);
    }

    @Override
    public GeneratedRecordComparator generateRecordComparator(
            TableConfig tableConfig, List<LogicalType> fieldTypes, String name) {
        return ComparatorCodeGenerator.gen(
                tableConfig,
                name,
                RowType.of(fieldTypes.toArray(new LogicalType[0])),
                SortUtil.getAscendingSortSpec(IntStream.range(0, fieldTypes.size()).toArray()));
    }
}
