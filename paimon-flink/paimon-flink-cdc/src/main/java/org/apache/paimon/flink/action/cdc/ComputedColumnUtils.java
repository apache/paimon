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

import org.apache.paimon.flink.action.cdc.utils.DfsSort;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

        // sort computed column args by dependencies
        LinkedHashMap<String, Tuple2<String, String[]>> sortedArgs =
                sortComputedColumnArgs(computedColumnArgs, caseSensitive);

        List<ComputedColumn> computedColumns = new ArrayList<>();
        for (Map.Entry<String, Tuple2<String, String[]>> columnArg : sortedArgs.entrySet()) {
            String columnName = columnArg.getKey().trim();
            String exprName = columnArg.getValue().f0.trim();
            String[] args = columnArg.getValue().f1;

            Expression expr = Expression.create(typeMapping, caseSensitive, exprName, args);
            ComputedColumn cmpColumn = new ComputedColumn(columnName, expr);
            computedColumns.add(new ComputedColumn(columnName, expr));

            // remember the column type for later reference by other computed columns
            typeMapping.put(columnName, cmpColumn.columnType());
        }

        return computedColumns;
    }

    private static LinkedHashMap<String, Tuple2<String, String[]>> sortComputedColumnArgs(
            List<String> computedColumnArgs, boolean caseSensitive) {
        List<String> argList =
                computedColumnArgs.stream()
                        .map(x -> caseSensitive ? x : x.toUpperCase())
                        .collect(Collectors.toList());

        LinkedHashMap<String, Tuple2<String, String[]>> eqMap = new LinkedHashMap<>();
        LinkedHashMap<String, String> refMap = new LinkedHashMap<>();
        for (String arg : argList) {
            String[] kv = arg.split("=");
            if (kv.length != 2) {
                throw new IllegalArgumentException(
                        String.format(
                                "Invalid computed column argument: %s. Please use format 'column-name=expr-name(args, ...)'.",
                                arg));
            }
            String expression = kv[1].trim();
            // parse expression
            int left = expression.indexOf('(');
            int right = expression.indexOf(')');
            Preconditions.checkArgument(
                    left > 0 && right > left,
                    String.format(
                            "Invalid expression: %s. Please use format 'expr-name(args, ...)'.",
                            expression));
            String exprName = expression.substring(0, left);
            String[] args = expression.substring(left + 1, right).split(",");

            // args[0] may be empty string, eg. "cal_col=now()"
            eqMap.put(kv[0].trim(), Tuple2.of(exprName, args));
            refMap.put(kv[0].trim(), args[0].trim());
        }

        List<String> sortedKeys = DfsSort.sortKeys(refMap);

        LinkedHashMap<String, Tuple2<String, String[]>> sortedMap =
                new LinkedHashMap<>(refMap.size());
        for (String key : sortedKeys) {
            sortedMap.put(key, eqMap.get(key));
        }
        return sortedMap;
    }
}
