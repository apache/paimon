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

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.DataEvolutionIndexSourceMeta;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.ProjectedDataFileMeta;
import org.apache.paimon.manifest.CompactFileIdentifierSet;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ProjectedManifestEntry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

import static org.apache.paimon.utils.DataEvolutionUtils.fieldMaxSequenceNumber;
import static org.apache.paimon.utils.DataEvolutionUtils.fileFields;

/** Plans existing global index files which need refresh after data-evolution updates. */
public final class DataEvolutionGlobalIndexRefreshPlanner {

    private DataEvolutionGlobalIndexRefreshPlanner() {}

    /**
     * Consumes data entries one by one. Entries are never retained; only merged row ranges bucketed
     * by distinct scan sequence numbers are kept in memory.
     */
    public static List<IndexManifestEntry> findIndexesToRefresh(
            SchemaManager schemaManager,
            Iterable<ManifestEntry> dataEntries,
            List<IndexManifestEntry> indexEntries,
            List<DataField> indexedFields) {
        Map<Pair<BinaryRow, Integer>, RefreshGroup> groups =
                collectRefreshGroups(indexEntries, indexedFields);
        if (groups.isEmpty()) {
            return Collections.emptyList();
        }

        Set<Integer> indexedFieldIds = indexedFieldIds(indexedFields);
        Function<Long, TableSchema> schemaLoader = cachedSchemaLoader(schemaManager);
        Map<Pair<Long, List<String>>, List<DataField>> fileFieldsCache = new HashMap<>();
        for (ManifestEntry dataEntry : dataEntries) {
            DataFileMeta file = dataEntry.file();
            if (dataEntry.kind() != FileKind.ADD || file.firstRowId() == null) {
                continue;
            }

            RefreshGroup group = groups.get(Pair.of(dataEntry.partition(), dataEntry.bucket()));
            if (group == null || !group.mayContainUpdate(file)) {
                continue;
            }

            addIfUpdatesIndexedFields(schemaLoader, fileFieldsCache, indexedFieldIds, group, file);
        }

        return collectMarkedIndexes(groups, indexEntries);
    }

    /**
     * Scans data manifests through reusable {@link ProjectedManifestEntry} views and plans indexes
     * to refresh. Neither {@link ManifestEntry} nor {@link DataFileMeta} POJOs are materialized:
     * one narrow DELETE pass tracks removed files in a primitive identifier set, then one projected
     * ADD pass merges updated row ranges directly into the refresh groups.
     */
    public static List<IndexManifestEntry> findIndexesToRefresh(
            FileStoreTable table,
            Snapshot snapshot,
            @Nullable PartitionPredicate partitionPredicate,
            List<IndexManifestEntry> indexEntries,
            List<DataField> indexedFields) {
        Map<Pair<BinaryRow, Integer>, RefreshGroup> groups =
                collectRefreshGroups(indexEntries, indexedFields);
        if (groups.isEmpty()) {
            return Collections.emptyList();
        }

        List<ManifestFileMeta> manifests =
                table.store()
                        .newScan()
                        .withPartitionFilter(partitionPredicate)
                        .manifestsReader()
                        .read(snapshot, ScanMode.ALL)
                        .filteredManifests;
        ManifestFile manifestFile = table.store().manifestFileFactory().create();
        Set<BinaryRow> groupPartitions = new HashSet<>();
        for (Pair<BinaryRow, Integer> key : groups.keySet()) {
            groupPartitions.add(key.getLeft());
        }

        CompactFileIdentifierSet deleted = new CompactFileIdentifierSet();
        try {
            collectDeletedIdentifiers(manifestFile, manifests, groupPartitions, deleted);
            collectUpdatedRanges(
                    table.schemaManager(),
                    manifestFile,
                    manifests,
                    deleted,
                    groups,
                    indexedFieldIds(indexedFields));
        } finally {
            deleted.release();
        }

        return collectMarkedIndexes(groups, indexEntries);
    }

    private static Map<Pair<BinaryRow, Integer>, RefreshGroup> collectRefreshGroups(
            List<IndexManifestEntry> indexEntries, List<DataField> indexedFields) {
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
        for (RefreshGroup group : groups.values()) {
            group.finishAddingIndexes();
        }
        return groups;
    }

    private static void collectDeletedIdentifiers(
            ManifestFile manifestFile,
            List<ManifestFileMeta> manifests,
            Set<BinaryRow> groupPartitions,
            CompactFileIdentifierSet deleted) {
        for (ManifestFileMeta manifest : manifests) {
            if (manifest.numDeletedFiles() <= 0) {
                continue;
            }
            try (CloseableIterator<ProjectedManifestEntry> entries =
                    manifestFile.scan(
                            manifest.fileName(), ProjectedManifestEntry.DELETE_ENTRY_PROJECTION)) {
                while (entries.hasNext()) {
                    ProjectedManifestEntry entry = entries.next();
                    if (entry.isDelete() && groupPartitions.contains(entry.partition())) {
                        deleted.add(entry);
                    }
                }
            } catch (Exception e) {
                throw manifestScanException(manifest, e);
            }
        }
    }

    private static void collectUpdatedRanges(
            SchemaManager schemaManager,
            ManifestFile manifestFile,
            List<ManifestFileMeta> manifests,
            CompactFileIdentifierSet deleted,
            Map<Pair<BinaryRow, Integer>, RefreshGroup> groups,
            Set<Integer> indexedFieldIds) {
        Function<Long, TableSchema> schemaLoader = cachedSchemaLoader(schemaManager);
        Map<Pair<Long, List<String>>, List<DataField>> fileFieldsCache = new HashMap<>();
        ProjectedManifestEntry.Projection projection = addedEntryProjection(!deleted.isEmpty());
        for (ManifestFileMeta manifest : manifests) {
            if (manifest.numAddedFiles() <= 0) {
                continue;
            }
            try (CloseableIterator<ProjectedManifestEntry> entries =
                    manifestFile.scan(manifest.fileName(), projection)) {
                while (entries.hasNext()) {
                    ProjectedManifestEntry entry = entries.next();
                    if (!entry.isAdd()) {
                        continue;
                    }
                    ProjectedDataFileMeta file = entry.file();
                    if (!file.hasFirstRowId()) {
                        continue;
                    }
                    RefreshGroup group = groups.get(Pair.of(entry.partition(), entry.bucket()));
                    if (group == null || !group.mayContainUpdate(file)) {
                        continue;
                    }
                    if (!deleted.isEmpty() && deleted.contains(entry)) {
                        continue;
                    }
                    addIfUpdatesIndexedFields(
                            schemaLoader, fileFieldsCache, indexedFieldIds, group, file);
                }
            } catch (Exception e) {
                throw manifestScanException(manifest, e);
            }
        }
    }

    private static void addIfUpdatesIndexedFields(
            Function<Long, TableSchema> schemaLoader,
            Map<Pair<Long, List<String>>, List<DataField>> fileFieldsCache,
            Set<Integer> indexedFieldIds,
            RefreshGroup group,
            DataFileMeta file) {
        List<DataField> physicalFields =
                fileFieldsCache.computeIfAbsent(
                        Pair.of(file.schemaId(), file.writeCols()),
                        key -> fileFields(schemaLoader, file));
        long[] columnSequences = file.columnMaxSequenceNumbers();
        long indexedMaxSequence = Long.MIN_VALUE;
        for (int position = 0; position < physicalFields.size(); position++) {
            if (indexedFieldIds.contains(physicalFields.get(position).id())) {
                indexedMaxSequence =
                        Math.max(
                                indexedMaxSequence,
                                fieldMaxSequenceNumber(
                                        file, columnSequences, position, physicalFields.size()));
            }
        }
        if (indexedMaxSequence != Long.MIN_VALUE) {
            group.addUpdatedFile(indexedMaxSequence, file.nonNullRowIdRange());
        }
    }

    private static Function<Long, TableSchema> cachedSchemaLoader(SchemaManager schemaManager) {
        Map<Long, TableSchema> schemaCache = new HashMap<>();
        return schemaId -> schemaCache.computeIfAbsent(schemaId, schemaManager::schema);
    }

    /**
     * Projects only the fields the refresh planner consumes; identifier fields are included only
     * when deleted files must be recognized.
     */
    private static ProjectedManifestEntry.Projection addedEntryProjection(
            boolean includeIdentifierFields) {
        List<DataField> fileFields = new ArrayList<>();
        fileFields.add(DataFileMeta.SCHEMA.getField(DataFileMeta.ROW_COUNT));
        fileFields.add(DataFileMeta.SCHEMA.getField(DataFileMeta.MAX_SEQUENCE_NUMBER));
        fileFields.add(DataFileMeta.SCHEMA.getField(DataFileMeta.SCHEMA_ID));
        fileFields.add(DataFileMeta.SCHEMA.getField(DataFileMeta.FIRST_ROW_ID));
        fileFields.add(DataFileMeta.SCHEMA.getField(DataFileMeta.WRITE_COLS));
        fileFields.add(DataFileMeta.SCHEMA.getField(DataFileMeta.WRITE_COLS_SEQUENCES));
        if (includeIdentifierFields) {
            fileFields.add(DataFileMeta.SCHEMA.getField(DataFileMeta.FILE_NAME));
            fileFields.add(DataFileMeta.SCHEMA.getField(DataFileMeta.LEVEL));
            fileFields.add(DataFileMeta.SCHEMA.getField(DataFileMeta.EXTRA_FILES));
            fileFields.add(DataFileMeta.SCHEMA.getField(DataFileMeta.EMBEDDED_FILE_INDEX));
            fileFields.add(DataFileMeta.SCHEMA.getField(DataFileMeta.EXTERNAL_PATH));
        }

        List<DataField> fields = new ArrayList<>();
        fields.add(ManifestEntry.MANIFEST_ROW_TYPE.getField(ManifestEntry.KIND));
        fields.add(ManifestEntry.MANIFEST_ROW_TYPE.getField(ManifestEntry.PARTITION));
        fields.add(ManifestEntry.MANIFEST_ROW_TYPE.getField(ManifestEntry.BUCKET));
        fields.add(
                ManifestEntry.MANIFEST_ROW_TYPE
                        .getField(ManifestEntry.FILE)
                        .newType(new RowType(false, fileFields)));
        return ProjectedManifestEntry.Projection.create(new RowType(false, fields));
    }

    private static Set<Integer> indexedFieldIds(List<DataField> indexedFields) {
        Set<Integer> indexedFieldIds = new HashSet<>();
        for (DataField field : indexedFields) {
            indexedFieldIds.add(field.id());
        }
        return indexedFieldIds;
    }

    private static List<IndexManifestEntry> collectMarkedIndexes(
            Map<Pair<BinaryRow, Integer>, RefreshGroup> groups,
            List<IndexManifestEntry> indexEntries) {
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

    private static RuntimeException manifestScanException(
            ManifestFileMeta manifest, Exception cause) {
        return new RuntimeException("Failed to scan manifest " + manifest.fileName(), cause);
    }

    private static final class RefreshGroup {

        private final List<IndexQuery> indexes = new ArrayList<>();
        private final MergedRanges indexedRanges = new MergedRanges();
        private long minScanSnapshotId = Long.MAX_VALUE;

        // Distinct scan sequence numbers in descending order. Bucket i merges row ranges of updated
        // data files whose max sequence number lies in
        // (sequenceNumbers[i], sequenceNumbers[i - 1]].
        private long[] sequenceNumbers;
        private MergedRanges[] updatedRangesPerSequenceNumber;

        private void addIndex(int ordinal, Range rowRange, long scanSnapshotId) {
            indexes.add(new IndexQuery(ordinal, rowRange, scanSnapshotId));
            indexedRanges.add(rowRange);
            minScanSnapshotId = Math.min(minScanSnapshotId, scanSnapshotId);
        }

        private void finishAddingIndexes() {
            indexes.sort((left, right) -> Long.compare(right.scanSnapshotId, left.scanSnapshotId));
            long[] distinct = new long[indexes.size()];
            int size = 0;
            for (IndexQuery index : indexes) {
                if (size == 0 || distinct[size - 1] != index.scanSnapshotId) {
                    distinct[size++] = index.scanSnapshotId;
                }
            }
            this.sequenceNumbers = Arrays.copyOf(distinct, size);
            this.updatedRangesPerSequenceNumber = new MergedRanges[size];
        }

        private boolean mayContainUpdate(DataFileMeta file) {
            return file.maxSequenceNumber() > minScanSnapshotId
                    && indexedRanges.intersects(file.nonNullRowIdRange());
        }

        private void addUpdatedFile(long maxSequenceNumber, Range rowRange) {
            if (maxSequenceNumber <= minScanSnapshotId) {
                return;
            }
            // Merge the range eagerly instead of retaining the file metadata.
            int sequenceNumberIndex = firstIndexWithSequenceNumberBelow(maxSequenceNumber);
            if (updatedRangesPerSequenceNumber[sequenceNumberIndex] == null) {
                updatedRangesPerSequenceNumber[sequenceNumberIndex] = new MergedRanges();
            }
            updatedRangesPerSequenceNumber[sequenceNumberIndex].add(rowRange);
        }

        /** Returns the first position whose scan sequence number is below the maximum sequence. */
        private int firstIndexWithSequenceNumberBelow(long maxSequenceNumber) {
            // mayContainUpdate guarantees the last sequence number qualifies.
            int low = 0;
            int high = sequenceNumbers.length - 1;
            while (low < high) {
                int mid = (low + high) >>> 1;
                if (sequenceNumbers[mid] < maxSequenceNumber) {
                    high = mid;
                } else {
                    low = mid + 1;
                }
            }
            return low;
        }

        private void markIndexesToRefresh(boolean[] result) {
            // As scan sequence numbers decrease, eligible updated ranges only grow.
            MergedRanges updatedRanges = null;
            int nextSequenceNumberIndex = 0;
            for (IndexQuery index : indexes) {
                while (nextSequenceNumberIndex < sequenceNumbers.length
                        && sequenceNumbers[nextSequenceNumberIndex] >= index.scanSnapshotId) {
                    MergedRanges ranges = updatedRangesPerSequenceNumber[nextSequenceNumberIndex];
                    if (ranges != null) {
                        updatedRanges =
                                updatedRanges == null ? ranges : updatedRanges.merge(ranges);
                        updatedRangesPerSequenceNumber[nextSequenceNumberIndex] = null;
                    }
                    nextSequenceNumberIndex++;
                }
                if (updatedRanges != null && updatedRanges.intersects(index.rowRange)) {
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
            add(range.from, range.to);
        }

        private void add(long from, long to) {
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

        /** Merges the smaller range set into the larger one and clears the source. */
        private MergedRanges merge(MergedRanges other) {
            MergedRanges target = this;
            MergedRanges source = other;
            if (target.ranges.size() < source.ranges.size()) {
                target = other;
                source = this;
            }
            for (Map.Entry<Long, Long> range : source.ranges.entrySet()) {
                target.add(range.getKey(), range.getValue());
            }
            source.ranges.clear();
            return target;
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
}
