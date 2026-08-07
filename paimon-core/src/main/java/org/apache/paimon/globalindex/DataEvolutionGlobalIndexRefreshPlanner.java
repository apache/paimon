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

package org.apache.paimon.globalindex;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.DataEvolutionIndexSourceMeta;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.paimon.utils.DataEvolutionUtils.fileFieldIds;

/** Plans existing global index files which need refresh after data-evolution updates. */
public final class DataEvolutionGlobalIndexRefreshPlanner {

    private DataEvolutionGlobalIndexRefreshPlanner() {}

    public static List<IndexManifestEntry> findIndexesToRefresh(
            SchemaManager schemaManager,
            List<ManifestEntry> dataEntries,
            List<IndexManifestEntry> indexEntries,
            List<DataField> indexedFields) {
        Set<Integer> indexedFieldIds = new HashSet<>();
        for (DataField field : indexedFields) {
            indexedFieldIds.add(field.id());
        }

        Map<Pair<BinaryRow, Integer>, RefreshGroup> groups = new HashMap<>();
        for (int i = 0; i < indexEntries.size(); i++) {
            IndexManifestEntry indexEntry = indexEntries.get(i);
            GlobalIndexMeta indexMeta = indexEntry.indexFile().globalIndexMeta();
            if (indexEntry.kind() != FileKind.ADD
                    || indexMeta == null
                    || !matchesFields(indexMeta, indexedFields)) {
                continue;
            }

            byte[] sourceMeta = indexMeta.sourceMeta();
            if (!DataEvolutionIndexSourceMeta.isDataEvolutionMeta(sourceMeta)) {
                // Legacy indexes have no trustworthy scan baseline and require an explicit rebuild.
                continue;
            }
            long scanSnapshotId =
                    DataEvolutionIndexSourceMeta.deserialize(sourceMeta).scanSnapshotId();
            groups.computeIfAbsent(
                            Pair.of(indexEntry.partition(), indexEntry.bucket()),
                            key -> new RefreshGroup())
                    .addIndex(i, indexMeta.rowRange(), scanSnapshotId);
        }

        Map<Pair<Long, List<String>>, Set<Integer>> fileFieldIdsCache = new HashMap<>();
        for (ManifestEntry dataEntry : dataEntries) {
            DataFileMeta file = dataEntry.file();
            if (dataEntry.kind() != FileKind.ADD || file.firstRowId() == null) {
                continue;
            }

            RefreshGroup group = groups.get(Pair.of(dataEntry.partition(), dataEntry.bucket()));
            if (group == null || !group.mayContainUpdate(file)) {
                continue;
            }

            Set<Integer> physicalFieldIds =
                    fileFieldIdsCache.computeIfAbsent(
                            Pair.of(file.schemaId(), file.writeCols()),
                            key -> fileFieldIds(schemaManager::schema, file));
            if (!disjoint(indexedFieldIds, physicalFieldIds)) {
                group.addDataFile(file);
            }
        }

        boolean[] indexesToRefresh = new boolean[indexEntries.size()];
        for (RefreshGroup group : groups.values()) {
            group.markIndexesToRefresh(indexesToRefresh);
        }

        List<IndexManifestEntry> result = new ArrayList<>();
        for (int i = 0; i < indexEntries.size(); i++) {
            if (indexesToRefresh[i]) {
                result.add(indexEntries.get(i));
            }
        }
        return result;
    }

    private static final class RefreshGroup {

        private final List<IndexQuery> indexes = new ArrayList<>();
        private final List<DataFileMeta> dataFiles = new ArrayList<>();
        private final MergedRanges indexedRanges = new MergedRanges();
        private long minScanSnapshotId = Long.MAX_VALUE;

        private void addIndex(int ordinal, Range rowRange, long scanSnapshotId) {
            indexes.add(new IndexQuery(ordinal, rowRange, scanSnapshotId));
            indexedRanges.add(rowRange);
            minScanSnapshotId = Math.min(minScanSnapshotId, scanSnapshotId);
        }

        private boolean mayContainUpdate(DataFileMeta file) {
            return file.maxSequenceNumber() > minScanSnapshotId
                    && indexedRanges.intersects(file.nonNullRowIdRange());
        }

        private void addDataFile(DataFileMeta file) {
            dataFiles.add(file);
        }

        private void markIndexesToRefresh(boolean[] result) {
            // As scan watermarks decrease, eligible data files only grow.
            dataFiles.sort(Comparator.comparingLong(DataFileMeta::maxSequenceNumber).reversed());
            indexes.sort((left, right) -> Long.compare(right.scanSnapshotId, left.scanSnapshotId));

            MergedRanges updatedRanges = new MergedRanges();
            int nextFile = 0;
            for (IndexQuery index : indexes) {
                while (nextFile < dataFiles.size()
                        && dataFiles.get(nextFile).maxSequenceNumber() > index.scanSnapshotId) {
                    updatedRanges.add(dataFiles.get(nextFile).nonNullRowIdRange());
                    nextFile++;
                }
                if (updatedRanges.intersects(index.rowRange)) {
                    result[index.ordinal] = true;
                }
            }
        }
    }

    private static final class IndexQuery {

        private final int ordinal;
        private final Range rowRange;
        private final long scanSnapshotId;

        private IndexQuery(int ordinal, Range rowRange, long scanSnapshotId) {
            this.ordinal = ordinal;
            this.rowRange = rowRange;
            this.scanSnapshotId = scanSnapshotId;
        }
    }

    /** Dynamically merged inclusive ranges supporting logarithmic intersection checks. */
    private static final class MergedRanges {

        private final NavigableMap<Long, Long> ranges = new TreeMap<>();

        private void add(Range range) {
            long from = range.from;
            long to = range.to;

            Map.Entry<Long, Long> floor = ranges.floorEntry(from);
            if (floor != null && floor.getValue() >= from) {
                from = floor.getKey();
                to = Math.max(to, floor.getValue());
                ranges.remove(floor.getKey());
            }

            Map.Entry<Long, Long> next = ranges.ceilingEntry(from);
            while (next != null && next.getKey() <= to) {
                to = Math.max(to, next.getValue());
                ranges.remove(next.getKey());
                next = ranges.ceilingEntry(from);
            }
            ranges.put(from, to);
        }

        private boolean intersects(Range range) {
            Map.Entry<Long, Long> floor = ranges.floorEntry(range.to);
            return floor != null && floor.getValue() >= range.from;
        }
    }

    private static boolean matchesFields(GlobalIndexMeta meta, List<DataField> fields) {
        if (fields.isEmpty() || meta.indexFieldId() != fields.get(0).id()) {
            return false;
        }
        int[] expectedExtraFields =
                fields.size() == 1
                        ? null
                        : fields.subList(1, fields.size()).stream()
                                .mapToInt(DataField::id)
                                .toArray();
        int[] actualExtraFields = meta.extraFieldIds();
        if (actualExtraFields == null || actualExtraFields.length == 0) {
            return expectedExtraFields == null || expectedExtraFields.length == 0;
        }
        return expectedExtraFields != null && Arrays.equals(actualExtraFields, expectedExtraFields);
    }

    private static boolean disjoint(Set<Integer> left, Set<Integer> right) {
        for (Integer value : left) {
            if (right.contains(value)) {
                return false;
            }
        }
        return true;
    }
}
