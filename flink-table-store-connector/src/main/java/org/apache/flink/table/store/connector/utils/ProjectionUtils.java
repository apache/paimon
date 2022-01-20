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

package org.apache.flink.table.store.connector.utils;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinaryRowDataUtil;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** Utils for {@link Projection}. */
public class ProjectionUtils {

    public static final Projection<RowData, BinaryRowData> EMPTY_PROJECTION =
            input -> BinaryRowDataUtil.EMPTY_ROW;

    public static RowType project(RowType inputType, int[] mapping) {
        List<RowType.RowField> fields = inputType.getFields();
        return new RowType(
                Arrays.stream(mapping).mapToObj(fields::get).collect(Collectors.toList()));
    }

    public static Projection<RowData, BinaryRowData> newProjection(
            RowType inputType, int[] mapping) {
        if (mapping.length == 0) {
            return EMPTY_PROJECTION;
        }

        @SuppressWarnings("unchecked")
        Projection<RowData, BinaryRowData> projection =
                ProjectionCodeGenerator.generateProjection(
                                CodeGeneratorContext.apply(new TableConfig()),
                                "Projection",
                                inputType,
                                project(inputType, mapping),
                                mapping)
                        .newInstance(Thread.currentThread().getContextClassLoader());
        return projection;
    }
}
