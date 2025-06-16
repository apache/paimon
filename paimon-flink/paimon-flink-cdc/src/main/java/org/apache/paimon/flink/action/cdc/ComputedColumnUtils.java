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

package org.apache.paimon.flink.action.cdc;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Utility methods for {@link ComputedColumn}, such as build. */
public class ComputedColumnUtils {

    public static List<ComputedColumn> buildComputedColumns(
            List<String> computedColumnArgs, List<DataField> physicFields) {
        return buildComputedColumns(computedColumnArgs, physicFields, true);
    }

    /** The caseSensitive only affects check. We don't change field names at building phase. */
    public static List<ComputedColumn> buildComputedColumns(
            List<String> computedColumnArgs, List<DataField> physicFields, boolean caseSensitive) {
        Map<String, DataType> typeMapping =
                physicFields.stream()
                        .collect(
                                Collectors.toMap(DataField::name, DataField::type, (v1, v2) -> v2));

        List<ComputedColumn> computedColumns = new ArrayList<>();
        for (String columnArg : computedColumnArgs) {
            String[] kv = columnArg.split("=");
            if (kv.length != 2) {
                throw new IllegalArgumentException(
                        String.format(
                                "Invalid computed column argument: %s. Please use format 'column-name=expr-name(args, ...)'.",
                                columnArg));
            }
            String columnName = kv[0].trim();
            String expression = kv[1].trim();
            // parse expression-
            int left = expression.indexOf('(');
            int right = expression.indexOf(')');
            Preconditions.checkArgument(
                    left > 0 && right > left,
                    String.format(
                            "Invalid expression: %s. Please use format 'expr-name(args, ...)'.",
                            expression));

            String exprName = expression.substring(0, left);
            String[] args = expression.substring(left + 1, right).split(",");
            checkArgument(args.length >= 1, "Computed column needs at least one argument.");

            computedColumns.add(
                    new ComputedColumn(
                            columnName,
                            Expression.create(typeMapping, caseSensitive, exprName, args)));
        }

        return sortComputedColumns(computedColumns);
    }

    @VisibleForTesting
    public static List<ComputedColumn> sortComputedColumns(List<ComputedColumn> columns) {
        Set<String> columnNames = new HashSet<>();
        for (ComputedColumn col : columns) {
            columnNames.add(col.columnName());
        }

        // For simple processing, no reference or referring to another computed column, means
        // independent
        List<ComputedColumn> independent = new ArrayList<>();
        List<ComputedColumn> dependent = new ArrayList<>();

        for (ComputedColumn col : columns) {
            if (col.fieldReference() == null || !columnNames.contains(col.fieldReference())) {
                independent.add(col);
            } else {
                dependent.add(col);
            }
        }

        // Sort dependent columns with topological sort
        Map<String, ComputedColumn> columnMap = new HashMap<>();
        Map<String, Set<String>> reverseDependencies = new HashMap<>();

        for (ComputedColumn col : dependent) {
            columnMap.put(col.columnName(), col);
            reverseDependencies
                    .computeIfAbsent(col.fieldReference(), k -> new HashSet<>())
                    .add(col.columnName());
        }

        List<ComputedColumn> sortedDependent = new ArrayList<>();
        Set<String> visited = new HashSet<>();
        Set<String> tempMark = new HashSet<>(); // For cycle detection

        for (ComputedColumn col : dependent) {
            if (!visited.contains(col.columnName())) {
                dfs(
                        col.columnName(),
                        reverseDependencies,
                        columnMap,
                        sortedDependent,
                        visited,
                        tempMark);
            }
        }

        Collections.reverse(sortedDependent);

        // Independent should precede dependent
        List<ComputedColumn> result = new ArrayList<>();
        result.addAll(independent);
        result.addAll(sortedDependent);

        return result;
    }

    private static void dfs(
            String node,
            Map<String, Set<String>> reverseDependencies,
            Map<String, ComputedColumn> columnMap,
            List<ComputedColumn> sorted,
            Set<String> visited,
            Set<String> tempMark) {
        if (tempMark.contains(node)) {
            throw new IllegalArgumentException("Cycle detected: " + node);
        }
        if (visited.contains(node)) {
            return;
        }

        tempMark.add(node);
        ComputedColumn current = columnMap.get(node);

        // Process the dependencies
        for (String dependent : reverseDependencies.getOrDefault(node, Collections.emptySet())) {
            dfs(dependent, reverseDependencies, columnMap, sorted, visited, tempMark);
        }

        tempMark.remove(node);
        visited.add(node);
        sorted.add(current);
    }
}
