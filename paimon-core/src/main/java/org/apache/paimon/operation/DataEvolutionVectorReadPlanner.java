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

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.operation.DataEvolutionSplitRead.VectorStoreBunchKey;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;

/** Plans ranges with fixed vector field providers before positional column merging. */
class DataEvolutionVectorReadPlanner {

    /**
     * Returns null when the existing sequential column-group readers suffice. {@code rowRanges} are
     * the pushed-down row ranges, or null when every row is read.
     */
    @Nullable
    static List<ReadRange> plan(
            List<DataFileMeta> files,
            RowType readType,
            Function<DataFileMeta, RowType> fileToRowType,
            @Nullable List<Range> rowRanges) {
        Set<Integer> readIds =
                readType.getFields().stream().map(DataField::id).collect(Collectors.toSet());
        boolean vectorOnly = files.stream().allMatch(file -> isVectorStoreFile(file.fileName()));
        Map<Integer, VectorStoreBunchKey> fieldGroups = new HashMap<>();
        Set<Integer> vectorReadIds = new HashSet<>();
        List<Candidate> candidates = new ArrayList<>();
        boolean overlappingGroups = false;
        Range logicalRange = null;
        long firstRowId = Long.MAX_VALUE;
        long lastRowId = Long.MIN_VALUE;
        for (DataFileMeta file : files) {
            Range range = file.nonNullRowIdRange();
            firstRowId = Math.min(firstRowId, range.from);
            lastRowId = Math.max(lastRowId, range.to);
            if (!isVectorStoreFile(file.fileName())) {
                if (!isBlobFile(file.fileName())) {
                    logicalRange = range;
                }
                continue;
            }
            RowType rowType = fileToRowType.apply(file);
            VectorStoreBunchKey key =
                    new VectorStoreBunchKey(
                            file.schemaId(),
                            DataFilePathFactory.formatIdentifier(file.fileName()),
                            file.writeCols(),
                            rowType);
            Set<Integer> fieldIds = new HashSet<>();
            for (DataField field : rowType.getFields()) {
                // Match historical fields by id: a rename must not create a different provider.
                VectorStoreBunchKey previous = fieldGroups.putIfAbsent(field.id(), key);
                overlappingGroups |= previous != null && !previous.equals(key);
                if (readIds.contains(field.id())) {
                    fieldIds.add(field.id());
                    vectorReadIds.add(field.id());
                }
            }
            if (!fieldIds.isEmpty() || vectorOnly) {
                candidates.add(new Candidate(file, fieldIds));
            }
        }

        if (logicalRange == null) {
            logicalRange = new Range(firstRowId, lastRowId);
        }

        // The sequential path reads each vector column as one VectorFileBunch, which must yield
        // exactly the group's rows. Otherwise plan explicit ranges, which NULL-fill uncovered rows
        // and take every row from its latest file. Rolled files that tile the group stay
        // sequential.
        boolean partialVector = false;
        for (int fieldId : vectorReadIds) {
            List<DataFileMeta> fieldFiles = new ArrayList<>();
            for (Candidate candidate : candidates) {
                if (candidate.fieldIds.contains(fieldId)) {
                    fieldFiles.add(candidate.file);
                }
            }
            if (!readsSequentially(fieldFiles, logicalRange, rowRanges)) {
                partialVector = true;
                break;
            }
        }

        // Keep the sequential path for disjoint column groups that each span the whole range.
        if (!overlappingGroups && !partialVector) {
            return null;
        }

        // Resolve the original files before VectorFileBunch can discard older overlapping files.
        // A bunch may contain different sequences in adjacent ranges, so sorting whole bunches
        // by their maximum sequence cannot establish the latest provider for every row.
        TreeMap<Long, List<Candidate>> boundaries = new TreeMap<>();
        boundaries.put(logicalRange.from, new ArrayList<>());
        boundaries.put(logicalRange.to + 1, new ArrayList<>());
        // Candidate ranges lie within the normal anchor, or the enclosing range computed above.
        for (Candidate candidate : candidates) {
            Range range = candidate.file.nonNullRowIdRange();
            boundaries.computeIfAbsent(range.from, ignored -> new ArrayList<>()).add(candidate);
            boundaries.computeIfAbsent(range.to + 1, ignored -> new ArrayList<>()).add(candidate);
        }

        TreeSet<Candidate> active =
                new TreeSet<>(
                        Comparator.<Candidate>comparingLong(c -> c.file.maxSequenceNumber())
                                .reversed()
                                .thenComparing(c -> c.file.fileName()));
        List<ReadRange> result = new ArrayList<>();
        long start = logicalRange.from;
        for (Map.Entry<Long, List<Candidate>> boundary : boundaries.entrySet()) {
            long end = boundary.getKey();
            if (start < end) {
                Set<Integer> assigned = new HashSet<>();
                List<DataFileMeta> providers = new ArrayList<>();
                for (Candidate candidate : active) {
                    // Presence in writeCols is what matters. An explicit NULL in the newest
                    // file must overwrite the old value, just like any other partial update.
                    if (assigned.addAll(candidate.fieldIds)) {
                        providers.add(candidate.file);
                    }
                    if (assigned.size() == vectorReadIds.size()) {
                        break;
                    }
                }
                if (providers.isEmpty() && vectorOnly && !active.isEmpty()) {
                    // Column pruning can remove the normal anchor. Retain a row-count provider
                    // for ranges where the projected vector has not been populated yet.
                    providers.add(active.first().file);
                }
                ReadRange previous = result.isEmpty() ? null : result.get(result.size() - 1);
                if (previous != null && previous.files.equals(providers)) {
                    previous.range = new Range(previous.range.from, end - 1);
                } else {
                    result.add(new ReadRange(new Range(start, end - 1), providers));
                }
            }
            // Each file enters at its first row id and leaves just after its last row id.
            for (Candidate candidate : boundary.getValue()) {
                if (!active.remove(candidate)) {
                    active.add(candidate);
                }
            }
            start = end;
        }
        return result;
    }

    /**
     * Whether a VectorFileBunch built from {@code files} yields exactly the rows of {@code target},
     * each from its latest file. The bunch takes files in first-row-id order, newest first, and
     * skips a file older than the one kept just before it, without comparing it to later kept
     * files. With row ranges pushed down, the bunch accepts gaps and every reader reads only the
     * selected rows, so a gap matters only if it contains a selected row.
     */
    private static boolean readsSequentially(
            List<DataFileMeta> files, Range target, @Nullable List<Range> rowRanges) {
        files.sort(
                Comparator.comparingLong(DataFileMeta::nonNullFirstRowId)
                        .thenComparing(
                                Comparator.comparingLong(DataFileMeta::maxSequenceNumber)
                                        .reversed()));
        List<DataFileMeta> kept = new ArrayList<>();
        List<DataFileMeta> skipped = new ArrayList<>();
        long cursor = target.from;
        long latestSequence = Long.MIN_VALUE;
        for (DataFileMeta file : files) {
            Range range = file.nonNullRowIdRange();
            if (range.from < cursor) {
                if (file.maxSequenceNumber() < latestSequence) {
                    skipped.add(file);
                    continue;
                }
                return false;
            }
            if (range.from > cursor && selects(cursor, range.from - 1, rowRanges)) {
                return false;
            }
            kept.add(file);
            cursor = range.to + 1;
            latestSequence = file.maxSequenceNumber();
        }
        if (cursor > target.to + 1 || selects(cursor, target.to, rowRanges)) {
            return false;
        }
        for (DataFileMeta older : skipped) {
            Range olderRange = older.nonNullRowIdRange();
            for (DataFileMeta file : kept) {
                Range range = file.nonNullRowIdRange();
                if (Range.intersect(olderRange.from, olderRange.to, range.from, range.to)
                        && file.maxSequenceNumber() <= older.maxSequenceNumber()) {
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean selects(long from, long to, @Nullable List<Range> rowRanges) {
        if (from > to) {
            return false;
        }
        if (rowRanges == null) {
            return true;
        }
        for (Range range : rowRanges) {
            if (Range.intersect(from, to, range.from, range.to)) {
                return true;
            }
        }
        return false;
    }

    static class ReadRange {

        Range range;
        final List<DataFileMeta> files;

        private ReadRange(Range range, List<DataFileMeta> files) {
            this.range = range;
            this.files = files;
        }
    }

    private static class Candidate {

        final DataFileMeta file;
        final Set<Integer> fieldIds;

        private Candidate(DataFileMeta file, Set<Integer> fieldIds) {
            this.file = file;
            this.fieldIds = fieldIds;
        }
    }
}
