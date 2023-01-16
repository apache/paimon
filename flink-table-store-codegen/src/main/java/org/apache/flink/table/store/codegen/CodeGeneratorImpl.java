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

import org.apache.flink.table.store.types.DataType;
import org.apache.flink.table.store.types.RowType;
import org.apache.flink.table.store.utils.TypeUtils;

import java.util.List;

/** Default implementation of {@link CodeGenerator}. */
public class CodeGeneratorImpl implements CodeGenerator {

    @Override
    public GeneratedClass<Projection> generateProjection(
            String name, RowType inputType, int[] inputMapping) {
        RowType outputType = TypeUtils.project(inputType, inputMapping);
        return ProjectionCodeGenerator.generateProjection(
                new CodeGeneratorContext(), name, inputType, outputType, inputMapping);
    }

    @Override
    public GeneratedClass<NormalizedKeyComputer> generateNormalizedKeyComputer(
            List<DataType> fieldTypes, String name) {
        return new SortCodeGenerator(
                        RowType.builder().fields(fieldTypes).build(),
                        getAscendingSortSpec(fieldTypes.size()))
                .generateNormalizedKeyComputer(name);
    }

    @Override
    public GeneratedClass<RecordComparator> generateRecordComparator(
            List<DataType> fieldTypes, String name) {
        return ComparatorCodeGenerator.gen(
                name,
                RowType.builder().fields(fieldTypes).build(),
                getAscendingSortSpec(fieldTypes.size()));
    }

    private SortSpec getAscendingSortSpec(int numFields) {
        SortSpec.SortSpecBuilder builder = SortSpec.builder();
        for (int i = 0; i < numFields; i++) {
            // Flink's default null collation is NullCollation.LOW, see FlinkPlannerImpl
            builder.addField(i, true, false);
        }
        return builder.build();
    }
}
