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

package org.apache.paimon.operation.commit;

import org.apache.paimon.Snapshot.CommitKind;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.sort.IndexedSortable;
import org.apache.paimon.sort.QuickSort;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.paimon.manifest.FileKind.ADD;
import static org.apache.paimon.manifest.FileKind.DELETE;
import static org.apache.paimon.operation.commit.DataEvolutionConflictDetection.dedicatedStorageFile;

/**
 * Successful-path DE checks retaining intervals rather than full file metadata. A possible conflict
 * is diagnosed by the ordinary checker after the caller closes its sorting resources. Interval
 * capacity and scope directories still grow with the input; this is not constant-memory storage.
 */
final class DataEvolutionStreamingChecks {

    private final boolean compact;
    private final boolean checkRanges;
    private final boolean checkBuckets;
    private final boolean checkIndexes;
    private final Intervals existing = new Intervals();
    private final Intervals normal = new Intervals();
    private final Intervals dedicated = new Intervals();
    private final Map<Pair<BinaryRow, Integer>, Intervals> existingByBucket = new HashMap<>();
    private final Map<Pair<BinaryRow, Integer>, Intervals> indexBase = new HashMap<>();
    private final Map<BinaryRow, Integer> rawBuckets = new HashMap<>();
    private final Map<BinaryRow, Integer> uniqueBuckets = new HashMap<>();
    private boolean rawBucketMismatch;
    private boolean uniqueBucketMismatch;
    private boolean invalid;

    DataEvolutionStreamingChecks(CommitKind kind, boolean checkRanges, boolean checkIndexes) {
        this.compact = kind == CommitKind.COMPACT;
        this.checkRanges = checkRanges;
        this.checkBuckets = kind != CommitKind.OVERWRITE;
        this.checkIndexes = checkIndexes;
    }

    void addRawBase(SimpleFileEntry entry) {
        if (checkBuckets) {
            rawBucketMismatch |= bucketMismatch(rawBuckets, entry);
        }
    }

    void addBase(SimpleFileEntry entry, long ordinal, @Nullable SimpleFileEntry lastDelete) {
        if (checkBuckets) {
            uniqueBucketMismatch |= bucketMismatch(uniqueBuckets, entry);
        }
        if (entry.kind() == ADD && lastDelete != null) {
            invalid |=
                    !Objects.equals(entry.firstRowId(), lastDelete.firstRowId())
                            || entry.rowCount() != lastDelete.rowCount();
        }
        if (entry.firstRowId() == null) {
            return;
        }
        long end = entry.firstRowId() + entry.rowCount() - 1;
        if (end < entry.firstRowId()) {
            invalid = true;
            return;
        }
        Range range = new Range(entry.firstRowId(), end);
        if (!dedicatedStorageFile(entry.fileName())) {
            if (compact) {
                existingByBucket
                        .computeIfAbsent(scope(entry), ignored -> new Intervals())
                        .add(range);
            } else {
                existing.add(range);
            }
        }
        if (checkIndexes && entry.kind() == ADD) {
            // Global indexes include dedicated base files, unlike ordinary existence checks.
            indexBase.computeIfAbsent(scope(entry), ignored -> new Intervals()).add(range);
        }
    }

    void addMerged(SimpleFileEntry entry) {
        if (entry.kind() == DELETE) {
            invalid = true;
        } else if (checkRanges && entry.firstRowId() != null) {
            long end = entry.firstRowId() + entry.rowCount() - 1;
            if (end < entry.firstRowId()) {
                invalid = true;
            } else {
                (dedicatedStorageFile(entry.fileName()) ? dedicated : normal)
                        .add(new Range(entry.firstRowId(), end));
            }
        }
    }

    boolean mayConflict(
            boolean deduplicateBase,
            List<ManifestEntry> delta,
            List<IndexManifestEntry> indexes,
            @Nullable Long nextRowId) {
        if (invalid
                || (checkBuckets && (deduplicateBase ? uniqueBucketMismatch : rawBucketMismatch))) {
            return true;
        }
        Map<BinaryRow, Integer> buckets = deduplicateBase ? uniqueBuckets : rawBuckets;
        existing.prepare(false);
        existingByBucket.values().forEach(ranges -> ranges.prepare(true));
        for (ManifestEntry entry : delta) {
            if (checkBuckets && bucketMismatch(buckets, entry)) {
                return true;
            }
            if (entry.kind() != ADD || entry.firstRowId() == null) {
                continue;
            }
            Range range;
            if (compact) {
                Intervals ranges = existingByBucket.get(scope(entry));
                if (ranges == null || !ranges.contains(entry.file().nonNullRowIdRange(), false)) {
                    return true;
                }
            } else if (nextRowId != null && entry.firstRowId() < nextRowId) {
                range = entry.file().nonNullRowIdRange();
                if (!existing.contains(range, !dedicatedStorageFile(entry.fileName()))) {
                    return true;
                }
            }
        }
        if (checkRanges) {
            if (normal.hasDifferentOverlaps()) {
                return true;
            }
            normal.prepare(false);
            for (int i = 0; i < dedicated.size; i++) {
                if (!normal.contains(new Range(dedicated.from(i), dedicated.to(i)), false)) {
                    return true;
                }
            }
        }
        if (checkIndexes) {
            indexBase.values().forEach(ranges -> ranges.prepare(true));
            for (IndexManifestEntry entry : indexes) {
                GlobalIndexMeta meta = entry.indexFile().globalIndexMeta();
                if (entry.kind() == ADD && meta != null) {
                    Intervals ranges = indexBase.get(Pair.of(entry.partition(), entry.bucket()));
                    if (ranges == null || !ranges.contains(meta.rowRange(), false)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static Pair<BinaryRow, Integer> scope(FileEntry entry) {
        return Pair.of(entry.partition(), entry.bucket());
    }

    private static boolean bucketMismatch(Map<BinaryRow, Integer> counts, FileEntry entry) {
        if (entry.totalBuckets() <= 0) {
            return false;
        }
        Integer previous = counts.putIfAbsent(entry.partition(), entry.totalBuckets());
        return previous != null && previous != entry.totalBuckets();
    }

    void clear() {
        existing.values = new long[0];
        normal.values = new long[0];
        dedicated.values = new long[0];
        existingByBucket.clear();
        indexBase.clear();
        rawBuckets.clear();
        uniqueBuckets.clear();
    }

    /** Pairs sorted with Paimon's existing sorter, then compacted in place. */
    private static final class Intervals implements IndexedSortable {
        private long[] values = new long[32];
        private int size;
        private boolean sorted;

        private void add(Range range) {
            if (size == (Integer.MAX_VALUE - 8) / 2) {
                throw new IllegalStateException("Too many DE conflict intervals.");
            }
            int required = (size + 1) * 2;
            if (required > values.length) {
                values =
                        Arrays.copyOf(
                                values,
                                (int) Math.min(Integer.MAX_VALUE - 8L, (long) values.length * 2));
            }
            values[size * 2] = range.from;
            values[size * 2 + 1] = range.to;
            size++;
            sorted = false;
        }

        private long from(int i) {
            return values[i * 2];
        }

        private long to(int i) {
            return values[i * 2 + 1];
        }

        private void sort() {
            if (!sorted && size > 1) {
                new QuickSort().sort(this);
            }
            sorted = true;
        }

        private boolean hasDifferentOverlaps() {
            sort();
            if (size == 0) {
                return false;
            }
            long start = from(0);
            long end = to(0);
            for (int i = 1; i < size; i++) {
                if (from(i) <= end) {
                    if (from(i) != start || to(i) != end) {
                        return true;
                    }
                } else {
                    start = from(i);
                    end = to(i);
                }
            }
            return false;
        }

        private void prepare(boolean adjacent) {
            sort();
            int result = 0;
            for (int i = 0; i < size; i++) {
                long start = from(i);
                long end = to(i);
                if (result > 0
                        && (start <= to(result - 1)
                                || (adjacent
                                        && to(result - 1) != Long.MAX_VALUE
                                        && start == to(result - 1) + 1))) {
                    values[(result - 1) * 2 + 1] = Math.max(to(result - 1), end);
                } else {
                    values[result * 2] = start;
                    values[result * 2 + 1] = end;
                    result++;
                }
            }
            size = result;
        }

        private boolean contains(Range range, boolean exact) {
            int low = 0;
            int high = size;
            while (low < high) {
                int middle = (low + high) >>> 1;
                if (to(middle) < range.from) {
                    low = middle + 1;
                } else {
                    high = middle;
                }
            }
            return low < size
                    && (exact
                            ? from(low) == range.from && to(low) == range.to
                            : from(low) <= range.from && to(low) >= range.to);
        }

        @Override
        public int compare(int i, int j) {
            int first = Long.compare(from(i), from(j));
            return first == 0 ? Long.compare(to(i), to(j)) : first;
        }

        @Override
        public void swap(int i, int j) {
            long first = from(i);
            long last = to(i);
            values[i * 2] = from(j);
            values[i * 2 + 1] = to(j);
            values[j * 2] = first;
            values[j * 2 + 1] = last;
        }

        // One logical segment with one-element address strides; no segmented byte storage.
        @Override
        public int compare(int si, int oi, int sj, int oj) {
            return compare(oi, oj);
        }

        @Override
        public void swap(int si, int oi, int sj, int oj) {
            swap(oi, oj);
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public int recordSize() {
            return 1;
        }

        @Override
        public int recordsPerSegment() {
            return Integer.MAX_VALUE;
        }
    }
}
