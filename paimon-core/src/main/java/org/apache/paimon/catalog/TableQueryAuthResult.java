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

package org.apache.paimon.catalog;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.And;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateVisitor;
import org.apache.paimon.predicate.Transform;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.source.DataFilePlan;
import org.apache.paimon.table.source.QueryAuthSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** Auth result for table query, including row level filter and optional column masking rules. */
public class TableQueryAuthResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final @Nullable List<String> filter;
    private final @Nullable Map<String, String> columnMasking;

    public TableQueryAuthResult(
            @Nullable List<String> filter, @Nullable Map<String, String> columnMasking) {
        this.filter = filter;
        this.columnMasking = columnMasking;
    }

    @Nullable
    public List<String> filter() {
        return filter;
    }

    @Nullable
    public Map<String, String> columnMasking() {
        return columnMasking;
    }

    public TableScan.Plan convertPlan(TableScan.Plan plan) {
        if (filter == null && (columnMasking == null || columnMasking.isEmpty())) {
            return plan;
        }
        List<Split> authSplits =
                plan.splits().stream()
                        .map(split -> new QueryAuthSplit(split, this))
                        .collect(Collectors.toList());
        return new DataFilePlan<>(authSplits);
    }

    @Nullable
    public Predicate extractPredicate() {
        Predicate rowFilter = null;
        if (filter != null && !filter.isEmpty()) {
            List<Predicate> predicates = new ArrayList<>();
            for (String json : filter) {
                if (StringUtils.isEmpty(json)) {
                    continue;
                }
                Predicate predicate = JsonSerdeUtil.fromJson(json, Predicate.class);
                if (predicate != null) {
                    predicates.add(predicate);
                }
            }
            if (predicates.size() == 1) {
                rowFilter = predicates.get(0);
            } else if (!predicates.isEmpty()) {
                rowFilter = new CompoundPredicate(And.INSTANCE, predicates);
            }
        }
        return rowFilter;
    }

    public Map<String, Transform> extractColumnMasking() {
        Map<String, Transform> result = new TreeMap<>();
        if (columnMasking != null && !columnMasking.isEmpty()) {
            for (Map.Entry<String, String> e : columnMasking.entrySet()) {
                String column = e.getKey();
                String json = e.getValue();
                if (StringUtils.isEmpty(column) || StringUtils.isEmpty(json)) {
                    continue;
                }
                Transform transform = JsonSerdeUtil.fromJson(json, Transform.class);
                if (transform == null) {
                    continue;
                }
                result.put(column, transform);
            }
        }
        return result;
    }

    public RecordReader<InternalRow> doAuth(
            RecordReader<InternalRow> reader, RowType outputRowType) {
        Predicate rowFilter = extractPredicate();
        if (rowFilter != null) {
            Predicate remappedFilter = rowFilter.visit(new PredicateRemapper(outputRowType));
            if (remappedFilter != null) {
                reader = reader.filter(remappedFilter::test);
            }
        }

        Map<String, Transform> columnMasking = extractColumnMasking();
        if (columnMasking != null && !columnMasking.isEmpty()) {
            Map<Integer, Transform> remappedMasking =
                    transformRemapping(outputRowType, columnMasking);
            if (!remappedMasking.isEmpty()) {
                reader = reader.transform(row -> transform(outputRowType, remappedMasking, row));
            }
        }

        return reader;
    }

    private static InternalRow transform(
            RowType outputRowType, Map<Integer, Transform> remappedMasking, InternalRow row) {
        int arity = outputRowType.getFieldCount();
        GenericRow out = new GenericRow(row.getRowKind(), arity);
        for (int i = 0; i < arity; i++) {
            DataType type = outputRowType.getTypeAt(i);
            out.setField(i, InternalRowUtils.get(row, i, type));
        }
        for (Map.Entry<Integer, Transform> e : remappedMasking.entrySet()) {
            int targetIndex = e.getKey();
            Transform transform = e.getValue();
            Object masked = transform.transform(row);
            out.setField(targetIndex, masked);
        }
        return out;
    }

    private static Map<Integer, Transform> transformRemapping(
            RowType outputRowType, Map<String, Transform> masking) {
        Map<Integer, Transform> out = new HashMap<>();
        if (masking == null || masking.isEmpty()) {
            return out;
        }

        for (Map.Entry<String, Transform> e : masking.entrySet()) {
            String targetColumn = e.getKey();
            Transform transform = e.getValue();
            if (targetColumn == null || transform == null) {
                continue;
            }

            int targetIndex = outputRowType.getFieldIndex(targetColumn);
            if (targetIndex < 0) {
                continue;
            }

            List<Object> newInputs = new ArrayList<>();
            for (Object input : transform.inputs()) {
                if (input instanceof FieldRef) {
                    FieldRef ref = (FieldRef) input;
                    int newIndex = outputRowType.getFieldIndex(ref.name());
                    if (newIndex < 0) {
                        throw new IllegalArgumentException(
                                "Column masking refers to field '"
                                        + ref.name()
                                        + "' which is not present in output row type "
                                        + outputRowType);
                    }
                    DataType type = outputRowType.getTypeAt(newIndex);
                    newInputs.add(new FieldRef(newIndex, ref.name(), type));
                } else {
                    newInputs.add(input);
                }
            }
            out.put(targetIndex, transform.copyWithNewInputs(newInputs));
        }
        return out;
    }

    private static class PredicateRemapper implements PredicateVisitor<Predicate> {

        private final RowType outputRowType;

        private PredicateRemapper(RowType outputRowType) {
            this.outputRowType = outputRowType;
        }

        @Override
        public Predicate visit(LeafPredicate predicate) {
            Transform transform = predicate.transform();
            List<Object> newInputs = new ArrayList<>();
            for (Object input : transform.inputs()) {
                if (input instanceof FieldRef) {
                    FieldRef ref = (FieldRef) input;
                    String fieldName = ref.name();
                    int newIndex = outputRowType.getFieldIndex(fieldName);
                    if (newIndex < 0) {
                        throw new RuntimeException(
                                String.format(
                                        "Unable to read data without column %s when row filter enabled.",
                                        fieldName));
                    }
                    DataType type = outputRowType.getTypeAt(newIndex);
                    newInputs.add(new FieldRef(newIndex, fieldName, type));
                } else {
                    newInputs.add(input);
                }
            }
            return predicate.copyWithNewInputs(newInputs);
        }

        @Override
        public Predicate visit(CompoundPredicate predicate) {
            List<Predicate> remappedChildren = new ArrayList<>();
            for (Predicate child : predicate.children()) {
                Predicate remapped = child.visit(this);
                if (remapped != null) {
                    remappedChildren.add(remapped);
                }
            }
            if (remappedChildren.isEmpty()) {
                return null;
            }
            if (remappedChildren.size() == 1) {
                return remappedChildren.get(0);
            }
            return new CompoundPredicate(predicate.function(), remappedChildren);
        }
    }
}
