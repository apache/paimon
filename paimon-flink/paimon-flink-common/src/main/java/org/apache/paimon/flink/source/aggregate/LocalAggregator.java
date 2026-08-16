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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.predicate.CompareUtils;
import org.apache.paimon.stats.SimpleStatsEvolutions;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ProjectedRow;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Local aggregator based on split statistics. */
class LocalAggregator {

    private final int[] grouping;
    private final List<Aggregate> aggregates;
    private final List<DataType> groupFieldTypes;
    private final List<String> groupFieldNames;
    private final ProjectedRow projectedPartition;
    private final InternalRowSerializer groupSerializer;

    private final Map<BinaryRow, List<AggFuncEvaluator>> groupEvaluatorMap;

    private final SimpleStatsEvolutions simpleStatsEvolutions;
    private final RowType resultRowType;
    private final InternalRowSerializer resultSerializer;

    LocalAggregator(FileStoreTable table, int[] grouping, List<Aggregate> aggregates) {
        // grouping has been converted to indices of the original table type
        this.grouping = grouping;
        this.aggregates = aggregates;
        this.groupFieldTypes = new ArrayList<>(grouping.length);
        this.groupFieldNames = new ArrayList<>(grouping.length);

        List<String> tableFieldNames = table.rowType().getFieldNames();
        List<String> partitionKeys = table.partitionKeys();
        RowType partitionType = table.rowType().project(partitionKeys);
        int[] partitionProjection = new int[grouping.length];
        for (int i = 0; i < grouping.length; i++) {
            String fieldName = tableFieldNames.get(grouping[i]);
            int partitionIndex = partitionKeys.indexOf(fieldName);
            partitionProjection[i] = partitionIndex;
            groupFieldTypes.add(partitionType.getTypeAt(partitionIndex));
            groupFieldNames.add(fieldName);
        }

        this.projectedPartition = ProjectedRow.from(partitionProjection);
        this.groupSerializer = new InternalRowSerializer(groupFieldTypes.toArray(new DataType[0]));
        this.groupEvaluatorMap = new LinkedHashMap<>();
        this.simpleStatsEvolutions =
                new SimpleStatsEvolutions(
                        schemaId -> table.schemaManager().schema(schemaId).fields(),
                        table.schema().id());
        this.resultRowType = createResultRowType();
        this.resultSerializer = new InternalRowSerializer(resultRowType);
    }

    void update(DataSplit dataSplit) {
        BinaryRow groupKey = groupKey(dataSplit.partition());
        List<AggFuncEvaluator> evaluators = groupEvaluatorMap.get(groupKey);
        if (evaluators == null) {
            evaluators = createEvaluators();
            groupEvaluatorMap.put(groupKey, evaluators);
        }
        update(evaluators, dataSplit);
    }

    List<InternalRow> result() {
        if (groupEvaluatorMap.isEmpty() && grouping.length == 0) {
            List<InternalRow> rows = new ArrayList<>(1);
            rows.add(createResultRow(BinaryRow.EMPTY_ROW, createEvaluators()));
            return rows;
        }

        List<InternalRow> rows = new ArrayList<>(groupEvaluatorMap.size());
        for (Map.Entry<BinaryRow, List<AggFuncEvaluator>> entry : groupEvaluatorMap.entrySet()) {
            rows.add(createResultRow(entry.getKey(), entry.getValue()));
        }
        return rows;
    }

    RowType resultRowType() {
        return resultRowType;
    }

    private BinaryRow groupKey(BinaryRow partition) {
        if (grouping.length == 0) {
            return BinaryRow.EMPTY_ROW;
        }
        return groupSerializer.toBinaryRow(projectedPartition.replaceRow(partition)).copy();
    }

    private void update(List<AggFuncEvaluator> evaluators, DataSplit dataSplit) {
        for (AggFuncEvaluator evaluator : evaluators) {
            evaluator.update(dataSplit);
        }
    }

    private List<AggFuncEvaluator> createEvaluators() {
        List<AggFuncEvaluator> evaluators = new ArrayList<>(aggregates.size());
        for (Aggregate aggregate : aggregates) {
            switch (aggregate.kind) {
                case COUNT:
                    evaluators.add(new CountStarEvaluator());
                    break;
                case MIN:
                    evaluators.add(new MinEvaluator(aggregate));
                    break;
                case MAX:
                    evaluators.add(new MaxEvaluator(aggregate));
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported aggregate " + aggregate.kind);
            }
        }
        return evaluators;
    }

    /**
     * According to {@code SupportsAggregatePushDown}, the produced data type will be strictly
     * organized by grouping keys and aggregate results with the same order as input.
     *
     * @return the result row
     */
    private InternalRow createResultRow(BinaryRow groupKey, List<AggFuncEvaluator> evaluators) {
        GenericRow row = new GenericRow(grouping.length + evaluators.size());
        for (int i = 0; i < grouping.length; i++) {
            row.setField(
                    i,
                    InternalRow.createFieldGetter(groupFieldTypes.get(i), i)
                            .getFieldOrNull(groupKey));
        }
        for (int i = 0; i < evaluators.size(); i++) {
            row.setField(grouping.length + i, evaluators.get(i).result());
        }
        return resultSerializer.copy(row);
    }

    private RowType createResultRowType() {
        List<DataField> fields = new ArrayList<>(groupFieldNames.size() + aggregates.size());
        for (int i = 0; i < groupFieldNames.size(); i++) {
            fields.add(new DataField(i, groupFieldNames.get(i), groupFieldTypes.get(i)));
        }
        for (int i = 0; i < aggregates.size(); i++) {
            Aggregate aggregate = aggregates.get(i);
            fields.add(
                    new DataField(
                            groupFieldNames.size() + i,
                            aggregate.resultName(),
                            aggregate.resultType()));
        }
        return new RowType(fields);
    }

    /** Aggregate information. */
    static class Aggregate {

        private final Kind kind;
        private final int fieldIndex;
        private final String fieldName;
        private final DataField dataField;

        private Aggregate(Kind kind, int fieldIndex, DataField dataField) {
            this.kind = kind;
            this.fieldIndex = fieldIndex;
            this.fieldName = dataField == null ? null : dataField.name();
            this.dataField = dataField;
        }

        static Aggregate count() {
            return new Aggregate(Kind.COUNT, -1, null);
        }

        static Aggregate min(int fieldIndex, DataField dataField) {
            return new Aggregate(Kind.MIN, fieldIndex, dataField);
        }

        static Aggregate max(int fieldIndex, DataField dataField) {
            return new Aggregate(Kind.MAX, fieldIndex, dataField);
        }

        boolean requiresMinMaxStats() {
            return kind == Kind.MIN || kind == Kind.MAX;
        }

        String fieldName() {
            return fieldName;
        }

        private DataType resultType() {
            switch (kind) {
                case COUNT:
                    return DataTypes.BIGINT().notNull();
                case MIN:
                case MAX:
                    return dataField.type();
                default:
                    throw new UnsupportedOperationException("Unsupported aggregate " + kind);
            }
        }

        private String resultName() {
            return kind.name().toLowerCase();
        }
    }

    /** Aggregate Kind. */
    private enum Kind {
        COUNT,
        MIN,
        MAX
    }

    /** Evaluator to calculate agg results from file statistics. */
    private interface AggFuncEvaluator {

        void update(DataSplit dataSplit);

        Object result();
    }

    /** Evaluator for count star. */
    private static class CountStarEvaluator implements AggFuncEvaluator {

        private long result;

        @Override
        public void update(DataSplit dataSplit) {
            result += dataSplit.mergedRowCount().getAsLong();
        }

        @Override
        public Object result() {
            return result;
        }
    }

    /** Evaluator for MIN. */
    private class MinEvaluator implements AggFuncEvaluator {

        private final Aggregate aggregate;
        private Object result;

        private MinEvaluator(Aggregate aggregate) {
            this.aggregate = aggregate;
        }

        @Override
        public void update(DataSplit dataSplit) {
            Object other =
                    dataSplit.minValue(
                            aggregate.fieldIndex, aggregate.dataField, simpleStatsEvolutions);
            if (other == null) {
                return;
            }

            if (result == null
                    || CompareUtils.compareLiteral(aggregate.dataField.type(), result, other) > 0) {
                result = other;
            }
        }

        @Override
        public Object result() {
            return result;
        }
    }

    /** Evaluator for MAX. */
    private class MaxEvaluator implements AggFuncEvaluator {

        private final Aggregate aggregate;
        private Object result;

        private MaxEvaluator(Aggregate aggregate) {
            this.aggregate = aggregate;
        }

        @Override
        public void update(DataSplit dataSplit) {
            Object other =
                    dataSplit.maxValue(
                            aggregate.fieldIndex, aggregate.dataField, simpleStatsEvolutions);
            if (other == null) {
                return;
            }

            if (result == null
                    || CompareUtils.compareLiteral(aggregate.dataField.type(), result, other) < 0) {
                result = other;
            }
        }

        @Override
        public Object result() {
            return result;
        }
    }
}
