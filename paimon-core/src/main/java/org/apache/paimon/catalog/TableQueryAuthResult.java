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
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.PredicateVisitor;
import org.apache.paimon.predicate.Transform;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.source.DataFilePlan;
import org.apache.paimon.table.source.QueryAuthSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.StringUtils;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** Auth result for table query, including row level filter and optional column masking rules. */
public class TableQueryAuthResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final @Nullable List<String> filter;
    private final @Nullable Map<String, String> columnMasking;

    // lazily parsed views of the JSON rules; transient so serialization stays unchanged
    private transient volatile Optional<Predicate> parsedFilter;
    private transient volatile Map<String, Transform> parsedMasking;

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

    /** Whether this result carries any effective row-filter or masking rule. */
    public boolean hasRules() {
        return extractPredicate() != null || !extractColumnMasking().isEmpty();
    }

    /**
     * Widens {@code readType} with the unprojected columns the rules read, or null when the
     * projection already covers them. Scans apply this before planning file pruning.
     */
    @Nullable
    public RowType widenReadType(RowType tableType, RowType readType) {
        return appendMissingFields(
                tableType, readType, requiredAuthFields(readType.getFieldNames()));
    }

    /**
     * Drops the conjuncts of {@code predicate} referencing any of {@code fields}; returns null when
     * nothing remains. Used to keep raw-statistics pushdown off masked columns.
     */
    @Nullable
    public static Predicate excludeFields(Predicate predicate, Set<String> fields) {
        return filterConjuncts(predicate, fields, true);
    }

    /**
     * Keeps only the conjuncts of {@code predicate} referencing any of {@code fields}; returns null
     * when none does.
     */
    @Nullable
    public static Predicate retainFields(Predicate predicate, Set<String> fields) {
        return filterConjuncts(predicate, fields, false);
    }

    @Nullable
    private static Predicate filterConjuncts(
            Predicate predicate, Set<String> fields, boolean keepDisjoint) {
        List<Predicate> kept = new ArrayList<>();
        for (Predicate conjunct : PredicateBuilder.splitAnd(predicate)) {
            if (Collections.disjoint(PredicateVisitor.collectFieldNames(conjunct), fields)
                    == keepDisjoint) {
                kept.add(conjunct);
            }
        }
        if (kept.isEmpty()) {
            return null;
        }
        return kept.size() == 1 ? kept.get(0) : PredicateBuilder.and(kept);
    }

    /** Appends the missing {@code ruleFields} of {@code tableType} to {@code readType}. */
    @Nullable
    public static RowType appendMissingFields(
            RowType tableType, RowType readType, Set<String> ruleFields) {
        List<DataField> widenedFields = null;
        for (DataField field : tableType.getFields()) {
            if (ruleFields.contains(field.name()) && !readType.containsField(field.name())) {
                if (widenedFields == null) {
                    widenedFields = new ArrayList<>(readType.getFields());
                }
                widenedFields.add(field);
            }
        }
        return widenedFields == null ? null : readType.copy(widenedFields);
    }

    public TableScan.Plan convertPlan(TableScan.Plan plan) {
        if (!hasRules()) {
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
        Optional<Predicate> parsed = parsedFilter;
        if (parsed == null) {
            parsed = Optional.ofNullable(parsePredicate());
            parsedFilter = parsed;
        }
        return parsed.orElse(null);
    }

    @Nullable
    private Predicate parsePredicate() {
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

    /**
     * Remap the auth predicate's field references by name to positional indices of {@code rowType}.
     * The auth server sends field-id-based {@link org.apache.paimon.predicate.FieldRef}s, which
     * must be resolved by name before positional evaluation.
     */
    @Nullable
    public static Predicate remapPredicate(Predicate predicate, RowType rowType) {
        return predicate.visit(new PredicateRemapper(rowType));
    }

    public Map<String, Transform> extractColumnMasking() {
        Map<String, Transform> parsed = parsedMasking;
        if (parsed == null) {
            parsed = parseColumnMasking();
            parsedMasking = parsed;
        }
        return parsed;
    }

    private Map<String, Transform> parseColumnMasking() {
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

    /**
     * Validates that every column the auth rules reference exists in the table's <b>latest</b>
     * schema (not a time-travel-pinned one). A rule keyed by a since-renamed column looks just like
     * an unprojected one at read time and would silently stop masking: fail closed instead.
     */
    public void validateAgainstSchema(RowType tableType, @Nullable List<String> projectedFields) {
        for (Map.Entry<String, Transform> entry : extractColumnMasking().entrySet()) {
            String target = entry.getKey();
            // a mask on an unprojected system column is inert (never in the output); don't reject
            if (SpecialFields.SYSTEM_FIELD_NAMES.contains(target)
                    && (projectedFields == null || !projectedFields.contains(target))) {
                continue;
            }
            checkFieldExists("Column masking", target, tableType, projectedFields);
            for (String input : PredicateVisitor.collectFieldNames(entry.getValue())) {
                checkFieldExists("Column masking", input, tableType, projectedFields);
            }
        }
        for (String operand : PredicateVisitor.collectFieldNames(extractPredicate())) {
            checkFieldExists("Row filter", operand, tableType, projectedFields);
        }
    }

    /**
     * Fails closed when a masked column is present in the read schema under a different name than
     * the rule uses (renamed between the read snapshot and latest): name-based enforcement would
     * skip the mask and leak the raw value. A column absent from the read schema is left inert.
     */
    public void validateReadableWithoutRename(RowType latestType, RowType readType) {
        for (Map.Entry<String, Transform> entry : extractColumnMasking().entrySet()) {
            checkNotRenamed(entry.getKey(), latestType, readType);
            // a mask whose target is absent from the read schema is inert; skip its inputs
            if (readType.containsField(entry.getKey())) {
                for (String input : PredicateVisitor.collectFieldNames(entry.getValue())) {
                    checkNotRenamed(input, latestType, readType);
                }
            }
        }
    }

    private static void checkNotRenamed(String field, RowType latestType, RowType readType) {
        // present by name: enforced fine. absent from latest: already thrown. else: renamed?
        if (readType.containsField(field) || !latestType.containsField(field)) {
            return;
        }
        int id = latestType.getField(field).id();
        if (readType.containsField(id)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Column masking references column '%s' which the snapshot being read "
                                    + "exposes as '%s' (renamed since); refusing to read to avoid "
                                    + "applying the rule by a stale name.",
                            field, readType.getField(id).name()));
        }
    }

    /**
     * The field names the auth rules read for a query projecting {@code projectedFields}: the
     * row-filter operands, plus (transitively) the inputs of every mask whose target is readable —
     * projected, or itself pulled in by the filter or another mask.
     */
    public Set<String> requiredAuthFields(List<String> projectedFields) {
        Map<String, Transform> masking = extractColumnMasking();
        Set<String> ruleFields = new HashSet<>();
        Set<String> readable = new HashSet<>(projectedFields);
        Deque<String> newlyReadable = new ArrayDeque<>(readable);
        for (String operand : PredicateVisitor.collectFieldNames(extractPredicate())) {
            ruleFields.add(operand);
            if (readable.add(operand)) {
                newlyReadable.add(operand);
            }
        }
        while (!newlyReadable.isEmpty()) {
            Transform mask = masking.get(newlyReadable.poll());
            if (mask == null) {
                continue;
            }
            for (String input : PredicateVisitor.collectFieldNames(mask)) {
                ruleFields.add(input);
                if (readable.add(input)) {
                    newlyReadable.add(input);
                }
            }
        }
        return ruleFields;
    }

    private static void checkFieldExists(
            String rule, String field, RowType tableType, @Nullable List<String> projectedFields) {
        // system fields (e.g. _ROW_ID) are readable metadata absent from the table schema,
        // but only when the query actually projects them -- they cannot be widened in
        if (SpecialFields.SYSTEM_FIELD_NAMES.contains(field)) {
            if (projectedFields != null && projectedFields.contains(field)) {
                return;
            }
            throw new IllegalArgumentException(
                    String.format(
                            "%s references system column '%s' which the query does not project.",
                            rule, field));
        }
        if (!tableType.containsField(field)) {
            throw new IllegalArgumentException(
                    String.format(
                            "%s references column '%s' which does not exist in table schema %s. "
                                    + "The rule may be stale after a column rename or drop; "
                                    + "refusing to read.",
                            rule, field, tableType.getFieldNames()));
        }
    }

    /**
     * Applies the row filter and column masking to {@code reader}. Rules are remapped by name;
     * masks apply only to targets in {@code activeFields}, the columns readable from the query.
     */
    public RecordReader<InternalRow> doAuth(
            RecordReader<InternalRow> reader, RowType outputRowType, Set<String> activeFields) {
        Predicate rowFilter = extractPredicate();
        if (rowFilter != null) {
            Predicate remappedFilter = remapPredicate(rowFilter, outputRowType);
            if (remappedFilter != null) {
                reader = reader.filter(remappedFilter::test);
            }
        }

        Map<String, Transform> columnMasking = extractColumnMasking();
        if (!columnMasking.isEmpty()) {
            Map<Integer, Transform> remappedMasking =
                    transformRemapping(outputRowType, columnMasking, activeFields);
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
            RowType outputRowType, Map<String, Transform> masking, Set<String> activeFields) {
        Map<Integer, Transform> out = new HashMap<>();
        for (Map.Entry<String, Transform> e : masking.entrySet()) {
            String targetColumn = e.getKey();
            Transform transform = e.getValue();
            if (targetColumn == null || transform == null) {
                continue;
            }

            if (!activeFields.contains(targetColumn)) {
                // not readable from the query, at most retained in a widened read schema
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
