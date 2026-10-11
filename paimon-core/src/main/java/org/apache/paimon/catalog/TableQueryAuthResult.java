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
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.PredicateRemapper;
import org.apache.paimon.predicate.PredicateVisitor;
import org.apache.paimon.predicate.Transform;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataFilePlan;
import org.apache.paimon.table.source.QueryAuthSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.ListUtils;
import org.apache.paimon.utils.StringUtils;
import org.apache.paimon.utils.TypeUtils;

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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Auth result for table query, including row level filter and optional column masking rules. */
public class TableQueryAuthResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final @Nullable List<String> filter;
    private final @Nullable Map<String, String> columnMasking;

    // Lazily parsed views of the JSON rules; transient so serialization stays unchanged. No
    // invalidation needed: an instance is immutable and rebuilt for every plan().
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

    public boolean hasRules() {
        return extractPredicate() != null || !extractColumnMasking().isEmpty();
    }

    /**
     * A catalog builds a fresh instance per authorization call and deserialization builds another,
     * so identity says nothing about whether two results agree.
     *
     * <p>Compared as the rules parse out rather than as they arrive, the filter as the set of its
     * conjuncts, so neither the JSON text nor the order the catalog lists them in matters.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TableQueryAuthResult)) {
            return false;
        }
        TableQueryAuthResult that = (TableQueryAuthResult) o;
        return parsedFilterConjuncts().equals(that.parsedFilterConjuncts())
                && extractColumnMasking().equals(that.extractColumnMasking());
    }

    @Override
    public int hashCode() {
        return Objects.hash(parsedFilterConjuncts(), extractColumnMasking());
    }

    private Set<Predicate> parsedFilterConjuncts() {
        Predicate predicate = extractPredicate();
        return predicate == null
                ? Collections.emptySet()
                : new HashSet<>(PredicateBuilder.splitAnd(predicate));
    }

    /**
     * Rejects a search on a query-auth table. Called from the methods that produce a scan or a
     * read, not from the builder constructors: the builders are serializable, and deserialization
     * would skip a constructor check.
     */
    public static void rejectSearchUnderQueryAuth(@Nullable Table table) {
        if (table instanceof FileStoreTable
                && ((FileStoreTable) table).coreOptions().queryAuthEnabled()) {
            throw new UnsupportedOperationException(
                    "Search is not supported on a query-auth table: the index ranks raw values, "
                            + "which a column mask invalidates.");
        }
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
        // and() folds an always-true/false leaf into a field-less constant, which would drop the
        // field names the callers match against; a lone conjunct must come back untouched
        return kept.size() == 1 ? kept.get(0) : PredicateBuilder.and(kept);
    }

    /**
     * Every column read by the conjuncts of {@code filter} that touch {@code maskTargets}. Their
     * unmasked operands count too, since splitAnd does not split a disjunction.
     */
    private static Set<String> postMaskFilterFields(
            @Nullable Predicate filter, Set<String> maskTargets) {
        if (filter == null || maskTargets.isEmpty()) {
            return Collections.emptySet();
        }
        Set<String> maskedInFilter = new HashSet<>(PredicateVisitor.collectFieldNames(filter));
        maskedInFilter.retainAll(maskTargets);
        if (maskedInFilter.isEmpty()) {
            return Collections.emptySet();
        }
        Predicate retained = retainFields(filter, maskedInFilter);
        return retained == null
                ? Collections.emptySet()
                : new HashSet<>(PredicateVisitor.collectFieldNames(retained));
    }

    /**
     * The columns a read projecting {@code readFields} under {@code filter} must additionally
     * expose: the rule fields, plus the operands of the conjuncts evaluated post-mask. The scan and
     * the read both widen by this, and must agree — the read schema is fixed on first use.
     */
    public Set<String> authFields(List<String> readFields, @Nullable Predicate filter) {
        Set<String> postMask = postMaskFilterFields(filter, extractColumnMasking().keySet());
        // requiredAuthFields de-duplicates, so a plain concatenation is enough here
        List<String> visible =
                postMask.isEmpty()
                        ? readFields
                        : ListUtils.union(readFields, new ArrayList<>(postMask));
        Set<String> ruleFields = requiredAuthFields(visible);
        // requiredAuthFields returns what the rules read, not the operands themselves
        ruleFields.addAll(postMask);
        return ruleFields;
    }

    @Nullable
    public static RowType appendMissingFields(
            RowType tableType, RowType readType, Set<String> ruleFields) {
        RowType widened = TypeUtils.withMissingFields(tableType, readType, ruleFields);
        return widened == readType ? null : widened;
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
                checkArgument(!StringUtils.isEmpty(json), "Row filter cannot be empty.");
                Predicate predicate = JsonSerdeUtil.fromJson(json, Predicate.class);
                checkArgument(predicate != null, "Row filter cannot be JSON null.");
                predicates.add(predicate);
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
        return PredicateRemapper.remap(predicate, rowType);
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
                checkArgument(!StringUtils.isEmpty(column), "Column mask target cannot be empty.");
                checkArgument(!StringUtils.isEmpty(json), "Column mask transform cannot be empty.");
                Transform transform = JsonSerdeUtil.fromJson(json, Transform.class);
                checkArgument(transform != null, "Column mask transform cannot be JSON null.");
                result.put(column, transform);
            }
        }
        // the cache is shared by every caller, so hand out a view that cannot rewrite the rules
        return Collections.unmodifiableMap(result);
    }

    /**
     * Validates that every column the auth rules reference exists in the table's <b>latest</b>
     * schema. A rule keyed by a since-renamed column would silently stop masking; fail closed.
     */
    public void validateAgainstSchema(RowType tableType, @Nullable List<String> projectedFields) {
        Map<String, Transform> masking = extractColumnMasking();
        for (Map.Entry<String, Transform> entry : masking.entrySet()) {
            String target = entry.getKey();
            // a mask on an unprojected system column never reaches the output; inert, don't reject
            if (SpecialFields.SYSTEM_FIELD_NAMES.contains(target)
                    && (projectedFields == null || !projectedFields.contains(target))) {
                continue;
            }
            checkFieldExists("Column masking", target, tableType, projectedFields);
            for (String input : PredicateVisitor.collectTransformFieldNames(entry.getValue())) {
                checkFieldExists("Column masking", input, tableType, projectedFields);
                // A transform reads the raw row, so an input that is itself masked would be
                // consumed unmasked and its raw value published through this target. Masking
                // the target of another mask is only self-consistent if composed, which the
                // read does not do; refuse the pair rather than leak.
                checkArgument(
                        input.equals(target) || !masking.containsKey(input),
                        "Column masking on '%s' reads column '%s', which is masked "
                                + "too. The mask would be computed from the raw value "
                                + "of '%s' and expose it through '%s'.",
                        target,
                        input,
                        input,
                        target);
            }
        }
        for (String operand : PredicateVisitor.collectFieldNames(extractPredicate())) {
            checkFieldExists("Row filter", operand, tableType, projectedFields);
        }
    }

    /**
     * Fails closed when the read schema exposes a rule's column under a different name, where
     * enforcing by name would skip it. A column absent from that schema stays inert.
     */
    public void validateReadableWithoutRename(RowType latestType, RowType readType) {
        for (Map.Entry<String, Transform> entry : extractColumnMasking().entrySet()) {
            checkNotRenamed("Column masking", entry.getKey(), latestType, readType);
            // a mask whose target is absent from the read schema is inert; skip its inputs
            if (readType.containsField(entry.getKey())) {
                for (String input : PredicateVisitor.collectTransformFieldNames(entry.getValue())) {
                    checkNotRenamed("Column masking", input, latestType, readType);
                }
            }
        }
        // the row filter is remapped by name as well, so it needs the same binding check
        for (String operand : PredicateVisitor.collectFieldNames(extractPredicate())) {
            checkNotRenamed("Row filter", operand, latestType, readType);
        }
    }

    private static void checkNotRenamed(
            String rule, String field, RowType latestType, RowType readType) {
        // absent from latest: already thrown by validateAgainstSchema
        if (!latestType.containsField(field)) {
            return;
        }
        int id = latestType.getField(field).id();
        if (readType.containsField(field)) {
            // a dropped and re-added column keeps the name but gets a fresh id, so the same
            // name may be an unrelated column in the snapshot being read
            checkArgument(
                    readType.getField(field).id() == id,
                    "%s references column '%s' which the snapshot being read exposes "
                            + "as a different column of the same name (dropped and "
                            + "re-added since); refusing to read to avoid applying the "
                            + "rule to unrelated data.",
                    rule,
                    field);
            return;
        }
        // not checkArgument: its arguments are evaluated eagerly, and getField(id) throws when
        // the id is absent, which is the normal case here
        if (readType.containsField(id)) {
            throw new IllegalArgumentException(
                    String.format(
                            "%s references column '%s' which the snapshot being read exposes as "
                                    + "'%s' (renamed since); refusing to read to avoid applying "
                                    + "the rule by a stale name.",
                            rule, field, readType.getField(id).name()));
        }
    }

    /**
     * The field names the auth rules read for a query projecting {@code projectedFields}: the
     * row-filter operands, plus transitively the inputs of every mask whose target is readable.
     */
    private Set<String> requiredAuthFields(List<String> projectedFields) {
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
            for (String input : PredicateVisitor.collectTransformFieldNames(mask)) {
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
        // system fields (e.g. _ROW_ID) cannot be widened in; only usable when projected
        if (SpecialFields.SYSTEM_FIELD_NAMES.contains(field)) {
            if (projectedFields != null && projectedFields.contains(field)) {
                return;
            }
            throw new IllegalArgumentException(
                    String.format(
                            "%s references system column '%s' which the query does not project.",
                            rule, field));
        }
        checkArgument(
                tableType.containsField(field),
                "%s references column '%s' which does not exist in table schema %s. "
                        + "The rule may be stale after a column rename or drop; "
                        + "refusing to read.",
                rule,
                field,
                tableType.getFieldNames());
    }

    /** Validate a physical projection before adding columns required by authorization rules. */
    public void validateReadType(
            RowType tableType,
            RowType readType,
            Set<String> ruleFields,
            Set<String> resolvedBlobViewFields) {
        Set<String> maskTargets = extractColumnMasking().keySet();
        for (String name : readType.getFieldNames()) {
            if (!ruleFields.contains(name) && !maskTargets.contains(name)) {
                continue;
            }
            if (!tableType.containsField(name)) {
                continue;
            }
            // rules must not touch a nested-pruned column (partial value)
            DataField tableField = tableType.getField(name);
            if (!readType.getField(name).type().equals(tableField.type())) {
                throw new IllegalStateException(
                        String.format(
                                "Query auth rules involve column '%s', which the query "
                                        + "projects with a pruned type %s instead of its "
                                        + "table type %s; cannot apply the rules to a "
                                        + "partial column.",
                                name, readType.getField(name).type(), tableField.type()));
            }
        }
        for (String name : ruleFields) {
            if (resolvedBlobViewFields.contains(name) && !readType.containsField(name)) {
                // auth-added columns bypass blob-view resolution
                throw new IllegalStateException(
                        String.format(
                                "Query auth rules read blob-view column '%s', which the query "
                                        + "does not project; such columns cannot be resolved. "
                                        + "Project the column or adjust the rule.",
                                name));
            }
        }
    }

    /**
     * Applies the row filter and column masking to {@code reader}. Rules are remapped by name;
     * masks apply only to targets in {@code activeFields}, the columns readable from the query.
     */
    public RecordReader<InternalRow> doAuth(
            RecordReader<InternalRow> reader, RowType outputRowType) {
        return doAuth(reader, outputRowType, extractPredicate(), extractColumnMasking());
    }

    /** Applies already decoded query-authorization definitions to a physical read projection. */
    public RecordReader<InternalRow> doAuth(
            RecordReader<InternalRow> reader,
            RowType outputRowType,
            @Nullable Predicate rowFilter,
            Map<String, Transform> selectedColumnMasking) {
        if (rowFilter != null) {
            Predicate remappedFilter = remapPredicate(rowFilter, outputRowType);
            if (remappedFilter != null) {
                reader = reader.filter(remappedFilter::test);
            }
        }

        if (!selectedColumnMasking.isEmpty()) {
            Map<Integer, Transform> remappedMasking =
                    transformRemapping(outputRowType, selectedColumnMasking);
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
        for (Map.Entry<String, Transform> e : masking.entrySet()) {
            String targetColumn = e.getKey();
            Transform transform = e.getValue();
            checkArgument(targetColumn != null, "Column mask target cannot be null.");
            checkArgument(transform != null, "Column mask transform cannot be null.");

            int targetIndex = outputRowType.getFieldIndex(targetColumn);
            checkArgument(
                    targetIndex >= 0,
                    "Column mask target '%s' is not present in output row type %s.",
                    targetColumn,
                    outputRowType);

            out.put(targetIndex, PredicateRemapper.remap(transform, outputRowType));
        }
        return out;
    }
}
