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

package org.apache.paimon.operation;

import org.apache.paimon.reader.DataEvolutionRow;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Pure (no-IO) planner for sub-field-level data evolution reads. Given the requested read row type
 * and, for each column-group file ("bunch"), the row type it physically provides (its written
 * columns, already wrapped with row-tracking fields), it decides for every read field whether it is
 * taken whole from a single file or composed sub-field by sub-field across several files (latest
 * file wins per leaf), and produces the offset maps and the per-field {@link
 * DataEvolutionRow.NestedField} assembly plans.
 *
 * <p>Separating this from {@link DataEvolutionSplitRead} keeps the reader-building (IO) thin and
 * lets the layout logic be unit-tested directly. Only one level of nested composition is supported;
 * deeper or cross-file splits of a sub-struct throw {@link UnsupportedOperationException}.
 */
class DataEvolutionReadPlanner {

    private final RowType readRowType;
    // for each bunch, the (row-tracked) row type it physically provides
    private final List<RowType> bunchAvailTypes;

    DataEvolutionReadPlanner(RowType readRowType, List<RowType> bunchAvailTypes) {
        this.readRowType = readRowType;
        this.bunchAvailTypes = bunchAvailTypes;
    }

    DataEvolutionReadPlan plan() {
        List<DataField> allReadFields = readRowType.getFields();
        int numFields = allReadFields.size();
        int numBunches = bunchAvailTypes.size();

        // gather, per bunch, the set of leaf field ids it physically provides
        List<Set<Integer>> bunchLeaves = new ArrayList<>();
        for (int i = 0; i < numBunches; i++) {
            Set<Integer> leaves = new HashSet<>();
            collectLeafIds(bunchAvailTypes.get(i).getFields(), leaves);
            bunchLeaves.add(leaves);
        }

        // decide, per read field, whether it is taken whole from one file or composed from several
        // files at sub-field granularity. Files are already sorted latest-first, so the first bunch
        // providing a leaf wins (latest-wins semantics, now at sub-field level).
        // selection per bunch: topFieldId -> null (whole) or set of selected sub-field ids
        List<Map<Integer, Set<Integer>>> bunchSelection = new ArrayList<>();
        for (int i = 0; i < numBunches; i++) {
            bunchSelection.add(new LinkedHashMap<>());
        }

        int[] rowOffsets = new int[numFields];
        int[] fieldOffsets = new int[numFields];
        Arrays.fill(rowOffsets, -1);
        Arrays.fill(fieldOffsets, -1);
        DataEvolutionRow.NestedField[] nested = new DataEvolutionRow.NestedField[numFields];
        boolean[] composite = new boolean[numFields];
        int[] wholeBunch = new int[numFields];
        Arrays.fill(wholeBunch, -1);

        for (int j = 0; j < numFields; j++) {
            DataField rf = allReadFields.get(j);
            List<Integer> leaves = leafIdsOf(rf);
            Map<Integer, Integer> leafProvider = new HashMap<>();
            Set<Integer> providers = new HashSet<>();
            for (int leaf : leaves) {
                int p = providerOf(leaf, bunchLeaves);
                if (p >= 0) {
                    leafProvider.put(leaf, p);
                    providers.add(p);
                }
            }
            if (providers.isEmpty()) {
                // no file provides this field; it stays null (nullability checked below)
                continue;
            }
            // Only read a field whole from a single file when that file covers ALL of its leaves.
            // If a single file provides only some leaves of a struct (the rest absent everywhere),
            // we must prune to the provided sub-fields so the reader is not asked for sub-fields
            // the
            // file does not physically contain; the missing ones stay null via the composite plan.
            boolean allLeavesCovered = leafProvider.size() == leaves.size();
            if (providers.size() == 1 && allLeavesCovered) {
                int b = providers.iterator().next();
                bunchSelection.get(b).put(rf.id(), null);
                wholeBunch[j] = b;
            } else {
                checkArgument(
                        rf.type() instanceof RowType,
                        "Field %s is split across files but is not a struct.",
                        rf.name());
                composite[j] = true;
                for (DataField sub : ((RowType) rf.type()).getFields()) {
                    List<Integer> subLeaves = leafIdsOf(sub);
                    Set<Integer> subProviders = new HashSet<>();
                    int coveredSubLeaves = 0;
                    for (int leaf : subLeaves) {
                        int p = leafProvider.getOrDefault(leaf, -1);
                        if (p >= 0) {
                            subProviders.add(p);
                            coveredSubLeaves++;
                        }
                    }
                    if (subProviders.size() > 1) {
                        throw new UnsupportedOperationException(
                                "Sub-field-level data evolution does not yet support splitting a "
                                        + "nested sub-field ("
                                        + rf.name()
                                        + "."
                                        + sub.name()
                                        + ") across multiple files.");
                    }
                    if (subProviders.size() == 1) {
                        if (sub.type() instanceof RowType && coveredSubLeaves < subLeaves.size()) {
                            // the single provider holds only part of this nested sub-struct;
                            // reading
                            // it whole would request leaves it lacks, and one-level composition
                            // cannot prune deeper than this level yet
                            throw new UnsupportedOperationException(
                                    "Sub-field-level data evolution does not yet support reading a "
                                            + "partially-written nested sub-field ("
                                            + rf.name()
                                            + "."
                                            + sub.name()
                                            + ") deeper than one level.");
                        }
                        int b = subProviders.iterator().next();
                        bunchSelection
                                .get(b)
                                .computeIfAbsent(rf.id(), k -> new LinkedHashSet<>())
                                .add(sub.id());
                    }
                    // else: sub-field absent everywhere -> stays null
                }
            }
        }

        // materialize each bunch's partial read row type and the offset maps.
        List<List<DataField>> bunchReadFields = new ArrayList<>();
        List<Map<Integer, Integer>> bunchTopOffset = new ArrayList<>();
        List<Map<Integer, Map<Integer, Integer>>> bunchSubOffset = new ArrayList<>();
        for (int i = 0; i < numBunches; i++) {
            Map<Integer, Set<Integer>> sel = bunchSelection.get(i);
            List<DataField> readFields = new ArrayList<>();
            Map<Integer, Integer> topOffset = new HashMap<>();
            Map<Integer, Map<Integer, Integer>> subOffset = new HashMap<>();
            for (Map.Entry<Integer, Set<Integer>> e : sel.entrySet()) {
                int topId = e.getKey();
                Set<Integer> subs = e.getValue();
                DataField readTop = readRowType.getField(topId);
                if (subs == null) {
                    readFields.add(readTop);
                    topOffset.put(topId, readFields.size() - 1);
                } else {
                    RowType readStruct = (RowType) readTop.type();
                    List<DataField> chosen = new ArrayList<>();
                    Map<Integer, Integer> subToIdx = new HashMap<>();
                    for (DataField s : readStruct.getFields()) {
                        if (subs.contains(s.id())) {
                            subToIdx.put(s.id(), chosen.size());
                            chosen.add(s);
                        }
                    }
                    RowType partial = new RowType(readStruct.isNullable(), chosen);
                    readFields.add(readTop.newType(partial));
                    topOffset.put(topId, readFields.size() - 1);
                    subOffset.put(topId, subToIdx);
                }
            }
            bunchReadFields.add(readFields);
            bunchTopOffset.add(topOffset);
            bunchSubOffset.add(subOffset);
        }

        // wire output offsets (whole fields) and nested composition plans (split structs).
        for (int j = 0; j < numFields; j++) {
            DataField rf = allReadFields.get(j);
            if (composite[j]) {
                List<DataField> subFields = ((RowType) rf.type()).getFields();
                int subCount = subFields.size();
                int[] subRowOffsets = new int[subCount];
                int[] subFieldOffsets = new int[subCount];
                Arrays.fill(subRowOffsets, -1);
                Arrays.fill(subFieldOffsets, -1);
                Map<Integer, Integer> bunchToPartial = new LinkedHashMap<>();
                List<int[]> partials = new ArrayList<>();
                for (int s = 0; s < subCount; s++) {
                    int subId = subFields.get(s).id();
                    int b = findSubProvider(rf.id(), subId, bunchSubOffset);
                    if (b < 0) {
                        // no file provides this sub-field; it stays null, so it must be nullable
                        checkArgument(
                                subFields.get(s).type().isNullable(),
                                "Sub-field %s.%s is not null but can't find any file contains it.",
                                rf.name(),
                                subFields.get(s).name());
                        continue;
                    }
                    Integer pIdx = bunchToPartial.get(b);
                    if (pIdx == null) {
                        int topOff = bunchTopOffset.get(b).get(rf.id());
                        int size = bunchSubOffset.get(b).get(rf.id()).size();
                        pIdx = partials.size();
                        bunchToPartial.put(b, pIdx);
                        partials.add(new int[] {b, topOff, size});
                    }
                    subRowOffsets[s] = pIdx;
                    subFieldOffsets[s] = bunchSubOffset.get(b).get(rf.id()).get(subId);
                }
                int p = partials.size();
                int[] pr = new int[p];
                int[] po = new int[p];
                int[] ps = new int[p];
                for (int k = 0; k < p; k++) {
                    pr[k] = partials.get(k)[0];
                    po[k] = partials.get(k)[1];
                    ps[k] = partials.get(k)[2];
                }
                nested[j] =
                        new DataEvolutionRow.NestedField(
                                pr, po, ps, subRowOffsets, subFieldOffsets);
            } else if (wholeBunch[j] >= 0) {
                int b = wholeBunch[j];
                rowOffsets[j] = b;
                fieldOffsets[j] = bunchTopOffset.get(b).get(rf.id());
            }
        }

        return new DataEvolutionReadPlan(rowOffsets, fieldOffsets, nested, bunchReadFields);
    }

    /** Collect (recursively) the leaf field ids of {@code fields}; only ROW types recurse. */
    private static void collectLeafIds(List<DataField> fields, Collection<Integer> out) {
        for (DataField f : fields) {
            if (f.type() instanceof RowType) {
                collectLeafIds(((RowType) f.type()).getFields(), out);
            } else {
                out.add(f.id());
            }
        }
    }

    private static List<Integer> leafIdsOf(DataField field) {
        List<Integer> result = new ArrayList<>();
        collectLeafIds(Collections.singletonList(field), result);
        return result;
    }

    private static int providerOf(int leafId, List<Set<Integer>> bunchLeaves) {
        for (int i = 0; i < bunchLeaves.size(); i++) {
            if (bunchLeaves.get(i).contains(leafId)) {
                return i;
            }
        }
        return -1;
    }

    private static int findSubProvider(
            int topId, int subId, List<Map<Integer, Map<Integer, Integer>>> bunchSubOffset) {
        for (int b = 0; b < bunchSubOffset.size(); b++) {
            Map<Integer, Integer> sm = bunchSubOffset.get(b).get(topId);
            if (sm != null && sm.containsKey(subId)) {
                return b;
            }
        }
        return -1;
    }

    /**
     * Immutable result of {@link DataEvolutionReadPlanner#plan()}: how each read field is sourced
     * (whole via {@code rowOffsets}/{@code fieldOffsets}, or composed via {@code nested}) and, per
     * bunch, the (possibly partial nested) fields to physically read.
     */
    static class DataEvolutionReadPlan {

        // per read field: the bunch a whole field is taken from (-1 if absent or composed)
        final int[] rowOffsets;
        // per read field: the field offset within that bunch's read row (-1 if absent or composed)
        final int[] fieldOffsets;
        // per read field: the sub-field assembly plan for a struct split across files (null if
        // whole)
        final DataEvolutionRow.NestedField[] nested;
        // per bunch: the fields (with partial nested structs) to read from that file
        final List<List<DataField>> bunchReadFields;

        DataEvolutionReadPlan(
                int[] rowOffsets,
                int[] fieldOffsets,
                DataEvolutionRow.NestedField[] nested,
                List<List<DataField>> bunchReadFields) {
            this.rowOffsets = rowOffsets;
            this.fieldOffsets = fieldOffsets;
            this.nested = nested;
            this.bunchReadFields = bunchReadFields;
        }
    }
}
