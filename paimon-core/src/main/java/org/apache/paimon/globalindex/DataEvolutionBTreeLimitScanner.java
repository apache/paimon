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
import org.apache.paimon.globalindex.btree.BTreeGlobalIndexerFactory;
import org.apache.paimon.globalindex.btree.BTreeIndexOptions;
import org.apache.paimon.globalindex.btree.BTreeIndexReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.index.DataEvolutionIndexSourceMeta;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.table.source.snapshot.TimeTravelUtil.tryTravelOrLatest;

/** A bounded equality lookup for fully covered, unchanged data-evolution BTree indexes. */
final class DataEvolutionBTreeLimitScanner {

    private static final int MAX_LIMIT = 1000;
    private static final int MAX_SNAPSHOTS_TO_CHECK = 32;

    private DataEvolutionBTreeLimitScanner() {}

    static Optional<GlobalIndexResult> scan(
            FileStoreTable table,
            @Nullable PartitionPredicate partitionFilter,
            LeafPredicate predicate,
            long limit) {
        if (limit <= 0
                || limit > MAX_LIMIT
                || predicate.function() != Equal.INSTANCE
                || !predicate.fieldRefOptional().isPresent()
                || predicate.literals().size() != 1
                || predicate.literals().get(0) == null) {
            return Optional.empty();
        }

        FieldRef fieldRef = predicate.fieldRefOptional().get();
        if (!table.rowType().containsField(fieldRef.name())) {
            return Optional.empty();
        }
        DataField field = table.rowType().getField(fieldRef.name());
        @Nullable Snapshot snapshot = tryTravelOrLatest(table);
        if (snapshot == null || snapshot.nextRowId() == null || snapshot.nextRowId() <= 0) {
            return Optional.empty();
        }

        List<IndexFileMeta> files =
                table.store().newIndexFileHandler()
                        .scan(snapshot, entry -> matchesIndex(entry, partitionFilter, field.id()))
                        .stream()
                        .map(IndexManifestEntry::indexFile)
                        .sorted(
                                Comparator.comparingLong(
                                        file -> file.globalIndexMeta().rowRangeStart()))
                        .collect(Collectors.toList());
        if (!hasCompleteFreshCoverage(table, snapshot, files)) {
            return Optional.empty();
        }

        KeySerializer serializer = KeySerializer.create(field.type());
        Comparator<Object> comparator = serializer.createComparator();
        Object literal = predicate.literals().get(0);
        IndexPathFactory pathFactory = table.store().pathFactory().globalIndexFileFactory();
        GlobalIndexFileReader fileReader = meta -> table.fileIO().newInputStream(meta.filePath());
        Options options = table.coreOptions().toConfiguration();
        RoaringNavigableMap64 matches = new RoaringNavigableMap64();
        try (CacheManager cache =
                new CacheManager(
                        options.get(BTreeIndexOptions.BTREE_INDEX_CACHE_SIZE),
                        options.get(BTreeIndexOptions.BTREE_INDEX_HIGH_PRIORITY_POOL_RATIO))) {
            for (IndexFileMeta file : files) {
                GlobalIndexMeta meta = file.globalIndexMeta();
                SortedIndexFileMeta sortedMeta = SortedIndexFileMeta.deserialize(meta.indexMeta());
                if (sortedMeta.firstKey() == null) {
                    continue;
                }
                Object firstKey = serializer.deserialize(MemorySlice.wrap(sortedMeta.firstKey()));
                Object lastKey = serializer.deserialize(MemorySlice.wrap(sortedMeta.lastKey()));
                if (comparator.compare(literal, firstKey) < 0
                        || comparator.compare(literal, lastKey) > 0) {
                    continue;
                }

                if (!sortedMeta.hasNulls() && comparator.compare(firstKey, lastKey) == 0) {
                    long remaining = limit - matches.getLongCardinality();
                    long count = Math.min(remaining, file.rowCount());
                    for (long offset = 0; offset < count; offset++) {
                        matches.add(meta.rowRangeStart() + offset);
                    }
                    if (matches.getLongCardinality() >= limit) {
                        break;
                    }
                    continue;
                }

                GlobalIndexIOMeta ioMeta =
                        new GlobalIndexIOMeta(
                                pathFactory.toPath(file),
                                file.fileSize(),
                                file.rowCount(),
                                meta.indexMeta());
                try (BTreeIndexReader reader =
                        new BTreeIndexReader(serializer, fileReader, ioMeta, cache)) {
                    GlobalIndexResult result =
                            reader.visitEqualWithLimit(
                                            literal, (int) (limit - matches.getLongCardinality()))
                                    .get();
                    for (long rowId : result.results()) {
                        matches.add(rowId + meta.rowRangeStart());
                    }
                }
                if (matches.getLongCardinality() >= limit) {
                    break;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read BTree index for limited equality.", e);
        }
        return Optional.of(GlobalIndexResult.create(matches));
    }

    private static boolean matchesIndex(
            IndexManifestEntry entry, @Nullable PartitionPredicate partitionFilter, int fieldId) {
        if (partitionFilter != null && !partitionFilter.test(entry.partition())) {
            return false;
        }
        IndexFileMeta file = entry.indexFile();
        GlobalIndexMeta meta = file.globalIndexMeta();
        return meta != null
                && meta.indexFieldId() == fieldId
                && BTreeGlobalIndexerFactory.IDENTIFIER.equals(file.indexType());
    }

    private static boolean hasCompleteFreshCoverage(
            FileStoreTable table, Snapshot snapshot, List<IndexFileMeta> files) {
        if (files.isEmpty()) {
            return false;
        }
        long nextRowId = snapshot.nextRowId();
        long nextRangeStart = 0;
        long sourceSnapshotId = -1;
        for (IndexFileMeta file : files) {
            GlobalIndexMeta meta = file.globalIndexMeta();
            if (meta.rowRangeStart() != nextRangeStart
                    || meta.rowRangeEnd() < nextRangeStart
                    || meta.rowRangeEnd() >= nextRowId
                    || file.rowCount() != meta.rowRangeEnd() - meta.rowRangeStart() + 1
                    || !DataEvolutionIndexSourceMeta.isDataEvolutionMeta(meta.sourceMeta())) {
                return false;
            }
            long fileSourceSnapshotId =
                    DataEvolutionIndexSourceMeta.fromIndexFile(file).scanSnapshotId();
            if (sourceSnapshotId != -1 && sourceSnapshotId != fileSourceSnapshotId) {
                return false;
            }
            sourceSnapshotId = fileSourceSnapshotId;
            nextRangeStart = meta.rowRangeEnd() + 1;
        }
        if (nextRangeStart != nextRowId
                || sourceSnapshotId > snapshot.id()
                || !table.snapshotManager().snapshotExists(sourceSnapshotId)) {
            return false;
        }

        Snapshot indexedSnapshot = table.snapshotManager().snapshot(sourceSnapshotId);
        if (snapshot.id() - sourceSnapshotId > MAX_SNAPSHOTS_TO_CHECK
                || snapshot.schemaId() != indexedSnapshot.schemaId()
                || !snapshot.nextRowId().equals(indexedSnapshot.nextRowId())) {
            return false;
        }

        // An index-only commit rewrites manifest lists. An empty APPEND cannot change data
        // files; inspect delta entries for any other kind of commit.
        SnapshotReader reader = null;
        for (long snapshotId = sourceSnapshotId + 1; snapshotId <= snapshot.id(); snapshotId++) {
            if (!table.snapshotManager().snapshotExists(snapshotId)) {
                return false;
            }
            Snapshot change = table.snapshotManager().snapshot(snapshotId);
            if (change.commitKind() == Snapshot.CommitKind.APPEND
                    && change.deltaRecordCount() == 0) {
                continue;
            }
            if (reader == null) {
                reader = table.newSnapshotReader().withMode(ScanMode.DELTA);
            }
            if (reader.withSnapshot(change).readFileIterator().hasNext()) {
                return false;
            }
        }
        return true;
    }
}
