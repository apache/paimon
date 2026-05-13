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

package org.apache.paimon.flink.source.aggregate;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Projection;

import org.apache.flink.table.expressions.AggregateExpression;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.table.source.PushDownUtils.minmaxAvailable;

/**
 * Utilities for aggregate push down.
 *
 * <p>This class references Spark's implementation: {@code
 * org.apache.paimon.spark.aggregate.AggregatePushDownUtils} and related classes.
 */
public class AggregatePushDownUtils {

    public static Optional<PushedAggregateResult> tryPushdownAggregation(
            FileStoreTable table,
            @Nullable Predicate predicate,
            @Nullable PartitionPredicate partitionPredicate,
            @Nullable int[][] projectFields,
            List<int[]> groupingSets,
            List<AggregateExpression> aggregateExpressions,
            org.apache.flink.table.types.DataType producedDataType) {
        if (groupingSets.size() != 1) {
            return Optional.empty();
        }

        // reject nested projection
        int[] fieldIndexMapping = createFieldIndexMapping(table, projectFields);
        if (fieldIndexMapping == null) {
            return Optional.empty();
        }

        // groupingSet contains the index within the projected fields,
        // so we have to translate grouping fields index to original field index
        int[] originalGrouping = translateFieldIndexes(groupingSets.get(0), fieldIndexMapping);
        if (originalGrouping == null) {
            return Optional.empty();
        }

        if (originalGrouping.length > 0
                && !groupingFieldsArePartitionFields(table, originalGrouping)) {
            return Optional.empty();
        }

        List<LocalAggregator.Aggregate> aggregates =
                extractAggregates(table, fieldIndexMapping, aggregateExpressions);
        if (aggregates == null) {
            return Optional.empty();
        }

        List<DataSplit> dataSplits = planSplits(table, predicate, partitionPredicate, aggregates);
        if (dataSplits == null) {
            return Optional.empty();
        }

        LocalAggregator aggregator = new LocalAggregator(table, originalGrouping, aggregates);
        for (DataSplit dataSplit : dataSplits) {
            aggregator.update(dataSplit);
        }

        // we should check the result row type equals to producedDataType
        RowType producedRowType = toPaimonRowType(producedDataType);
        if (producedRowType == null || !isCompatible(aggregator.resultRowType(), producedRowType)) {
            return Optional.empty();
        }
        return Optional.of(new PushedAggregateResult(aggregator.result(), producedRowType));
    }

    @Nullable
    private static RowType toPaimonRowType(org.apache.flink.table.types.DataType producedDataType) {
        if (!(producedDataType.getLogicalType()
                instanceof org.apache.flink.table.types.logical.RowType)) {
            return null;
        }
        return LogicalTypeConversion.toDataType(
                (org.apache.flink.table.types.logical.RowType) producedDataType.getLogicalType());
    }

    private static boolean isCompatible(RowType actualRowType, RowType producedRowType) {
        if (actualRowType.getFieldCount() != producedRowType.getFieldCount()) {
            return false;
        }

        for (int i = 0; i < actualRowType.getFieldCount(); i++) {
            DataType actualType = actualRowType.getTypeAt(i);
            DataType producedType = producedRowType.getTypeAt(i);
            if (!actualType.equalsIgnoreNullable(producedType)
                    || (actualType.isNullable() && !producedType.isNullable())) {
                return false;
            }
        }
        return true;
    }

    @Nullable
    private static List<DataSplit> planSplits(
            FileStoreTable table,
            @Nullable Predicate predicate,
            @Nullable PartitionPredicate partitionPredicate,
            List<LocalAggregator.Aggregate> aggregates) {
        Set<String> minMaxColumns =
                aggregates.stream()
                        .filter(LocalAggregator.Aggregate::requiresMinMaxStats)
                        .map(LocalAggregator.Aggregate::fieldName)
                        .collect(Collectors.toSet());
        if (!minMaxColumns.isEmpty()
                && (CoreOptions.fromMap(table.options()).deletionVectorsEnabled()
                        || !table.primaryKeys().isEmpty())) {
            return null;
        }

        ReadBuilder readBuilder =
                table.newReadBuilder()
                        .withFilter(predicate)
                        .withPartitionFilter(partitionPredicate);
        if (minMaxColumns.isEmpty()) {
            readBuilder.dropStats();
        }

        List<Split> splits = readBuilder.newScan().plan().splits();
        List<DataSplit> dataSplits = new ArrayList<>(splits.size());
        for (Split split : splits) {
            if (!(split instanceof DataSplit)) {
                return null;
            }

            OptionalLong mergedRowCount = split.mergedRowCount();
            if (!mergedRowCount.isPresent()) {
                return null;
            }

            if (!minMaxColumns.isEmpty() && !minmaxAvailable(split, minMaxColumns)) {
                return null;
            }

            dataSplits.add((DataSplit) split);
        }
        return dataSplits;
    }

    @Nullable
    private static List<LocalAggregator.Aggregate> extractAggregates(
            FileStoreTable table,
            int[] fieldIndexMapping,
            List<AggregateExpression> aggregateExpressions) {
        List<LocalAggregator.Aggregate> aggregates = new ArrayList<>(aggregateExpressions.size());
        for (AggregateExpression aggregateExpression : aggregateExpressions) {
            if (aggregateExpression.isDistinct()
                    || aggregateExpression.isApproximate()
                    || aggregateExpression.getFilterExpression().isPresent()) {
                return null;
            }

            String functionName = aggregateExpression.getFunctionDefinition().getClass().getName();
            if (isCountStar(functionName)) {
                aggregates.add(LocalAggregator.Aggregate.count());
            } else if (isMin(functionName) || isMax(functionName)) {
                if (aggregateExpression.getArgs().size() != 1) {
                    return null;
                }

                int originalFieldIndex =
                        toOriginalFieldIndex(
                                fieldIndexMapping,
                                aggregateExpression.getArgs().get(0).getFieldIndex());
                if (originalFieldIndex < 0) {
                    return null;
                }

                DataField field = table.rowType().getFields().get(originalFieldIndex);
                if (!minmaxAvailable(field.type())) {
                    return null;
                }

                aggregates.add(
                        isMin(functionName)
                                ? LocalAggregator.Aggregate.min(originalFieldIndex, field)
                                : LocalAggregator.Aggregate.max(originalFieldIndex, field));
            } else {
                return null;
            }
        }
        return aggregates;
    }

    private static boolean isCountStar(String functionName) {
        return functionName.equals(
                "org.apache.flink.table.planner.functions.aggfunctions.Count1AggFunction");
    }

    private static boolean isMin(String functionName) {
        return functionName.startsWith(
                "org.apache.flink.table.planner.functions.aggfunctions.MinAggFunction");
    }

    private static boolean isMax(String functionName) {
        return functionName.startsWith(
                "org.apache.flink.table.planner.functions.aggfunctions.MaxAggFunction");
    }

    private static boolean groupingFieldsArePartitionFields(
            FileStoreTable table, int[] originalGrouping) {
        List<String> tableFieldNames = table.rowType().getFieldNames();
        List<String> partitionKeys = table.partitionKeys();
        for (int originalFieldIndex : originalGrouping) {
            if (originalFieldIndex < 0 || originalFieldIndex >= tableFieldNames.size()) {
                return false;
            }
            if (!partitionKeys.contains(tableFieldNames.get(originalFieldIndex))) {
                return false;
            }
        }
        return true;
    }

    @Nullable
    private static int[] createFieldIndexMapping(
            FileStoreTable table, @Nullable int[][] projectFields) {
        int fieldCount = table.rowType().getFieldCount();
        int[] mapping =
                projectFields == null
                        ? Projection.range(0, fieldCount).toTopLevelIndexes()
                        : toTopLevelIndexes(projectFields);
        if (mapping == null) {
            return null;
        }

        for (int originalFieldIndex : mapping) {
            if (originalFieldIndex < 0 || originalFieldIndex >= fieldCount) {
                return null;
            }
        }
        return mapping;
    }

    @Nullable
    private static int[] toTopLevelIndexes(int[][] projectFields) {
        Projection projection = Projection.of(projectFields);
        if (projection.isNested()) {
            return null;
        }
        return projection.toTopLevelIndexes();
    }

    @Nullable
    private static int[] translateFieldIndexes(int[] fieldIndexes, int[] fieldIndexMapping) {
        int[] originalFieldIndexes = new int[fieldIndexes.length];
        for (int i = 0; i < fieldIndexes.length; i++) {
            int originalFieldIndex = toOriginalFieldIndex(fieldIndexMapping, fieldIndexes[i]);
            if (originalFieldIndex < 0) {
                return null;
            }
            originalFieldIndexes[i] = originalFieldIndex;
        }
        return originalFieldIndexes;
    }

    private static int toOriginalFieldIndex(int[] fieldIndexMapping, int fieldIndex) {
        if (fieldIndex < 0 || fieldIndex >= fieldIndexMapping.length) {
            return -1;
        }
        return fieldIndexMapping[fieldIndex];
    }
}
