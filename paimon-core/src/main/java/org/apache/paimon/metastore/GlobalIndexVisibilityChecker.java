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

package org.apache.paimon.metastore;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Range;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;

/** Checks whether newly added data files are covered by existing global indexes. */
class GlobalIndexVisibilityChecker {

    private final FileStoreTable table;
    private final Map<BinaryRow, List<Range>> rowIdRangesByPartition;
    private final Map<BinaryRow, Set<GlobalIndexIdentifier>> globalIndexesByPartition;

    private GlobalIndexVisibilityChecker(
            FileStoreTable table,
            Map<BinaryRow, List<Range>> rowIdRangesByPartition,
            Map<BinaryRow, Set<GlobalIndexIdentifier>> globalIndexesByPartition) {
        this.table = table;
        this.rowIdRangesByPartition = rowIdRangesByPartition;
        this.globalIndexesByPartition = globalIndexesByPartition;
    }

    static GlobalIndexVisibilityChecker create(
            FileStoreTable table, Snapshot snapshot, List<ManifestEntry> deltaFiles) {
        Map<BinaryRow, List<Range>> rowIdRangesByPartition =
                collectRowIdRangesByPartition(deltaFiles);
        Map<BinaryRow, Set<GlobalIndexIdentifier>> globalIndexesByPartition =
                rowIdRangesByPartition.isEmpty()
                        ? new HashMap<>()
                        : collectGlobalIndexesByPartition(
                                table, snapshot, rowIdRangesByPartition.keySet());
        return new GlobalIndexVisibilityChecker(
                table, rowIdRangesByPartition, globalIndexesByPartition);
    }

    boolean noNeedToWait() {
        return globalIndexesByPartition.isEmpty();
    }

    boolean visibleIn(Snapshot snapshot) {
        Map<BinaryRow, Map<GlobalIndexIdentifier, List<Range>>> indexedRangesByPartition =
                new HashMap<>();
        for (IndexManifestEntry entry : scanGlobalIndexes(table, snapshot)) {
            GlobalIndexMeta globalIndex = entry.indexFile().globalIndexMeta();
            if (globalIndex == null) {
                continue;
            }

            Set<GlobalIndexIdentifier> identifiers =
                    globalIndexesByPartition.get(entry.partition());
            if (identifiers == null) {
                continue;
            }

            GlobalIndexIdentifier identifier = identifierOf(entry);
            if (identifiers.contains(identifier)) {
                indexedRangesByPartition
                        .computeIfAbsent(entry.partition().copy(), k -> new HashMap<>())
                        .computeIfAbsent(identifier, k -> new ArrayList<>())
                        .add(globalIndex.rowRange());
            }
        }

        for (Map.Entry<BinaryRow, Set<GlobalIndexIdentifier>> partitionIndexes :
                globalIndexesByPartition.entrySet()) {
            BinaryRow partition = partitionIndexes.getKey();
            List<Range> rowIdRanges = rowIdRangesByPartition.get(partition);
            Map<GlobalIndexIdentifier, List<Range>> indexedRanges =
                    indexedRangesByPartition.get(partition);
            for (GlobalIndexIdentifier identifier : partitionIndexes.getValue()) {
                List<Range> ranges =
                        Range.sortAndMergeOverlap(
                                indexedRanges == null ? null : indexedRanges.get(identifier), true);
                for (Range rowIdRange : rowIdRanges) {
                    if (!rowIdRange.exclude(ranges).isEmpty()) {
                        return false;
                    }
                }
            }
        }

        return true;
    }

    private static Map<BinaryRow, List<Range>> collectRowIdRangesByPartition(
            List<ManifestEntry> deltaFiles) {
        Map<BinaryRow, List<Range>> rangesByPartition = new HashMap<>();
        for (ManifestEntry entry : deltaFiles) {
            if (shouldTrackGlobalIndex(entry)) {
                rangesByPartition
                        .computeIfAbsent(entry.partition().copy(), k -> new ArrayList<>())
                        .add(entry.file().nonNullRowIdRange());
            }
        }
        for (Map.Entry<BinaryRow, List<Range>> entry : rangesByPartition.entrySet()) {
            entry.setValue(Range.sortAndMergeOverlap(entry.getValue(), true));
        }
        return rangesByPartition;
    }

    private static boolean shouldTrackGlobalIndex(ManifestEntry entry) {
        if (!FileKind.ADD.equals(entry.kind())) {
            return false;
        }

        DataFileMeta file = entry.file();
        if (file.firstRowId() == null || file.rowCount() <= 0) {
            return false;
        }

        Optional<FileSource> fileSource = file.fileSource();
        if (!fileSource.isPresent() || !FileSource.APPEND.equals(fileSource.get())) {
            return false;
        }

        return !isBlobFile(file.fileName());
    }

    private static Map<BinaryRow, Set<GlobalIndexIdentifier>> collectGlobalIndexesByPartition(
            FileStoreTable table, Snapshot snapshot, Set<BinaryRow> partitionsToTrack) {
        Map<BinaryRow, Set<GlobalIndexIdentifier>> indexesByPartition = new HashMap<>();
        for (IndexManifestEntry entry : scanGlobalIndexes(table, snapshot)) {
            GlobalIndexMeta globalIndex = entry.indexFile().globalIndexMeta();
            if (globalIndex != null && partitionsToTrack.contains(entry.partition())) {
                indexesByPartition
                        .computeIfAbsent(entry.partition().copy(), k -> new HashSet<>())
                        .add(identifierOf(entry));
            }
        }
        return indexesByPartition;
    }

    private static List<IndexManifestEntry> scanGlobalIndexes(
            FileStoreTable table, Snapshot snapshot) {
        return table.store()
                .newIndexFileHandler()
                .scan(snapshot, entry -> entry.indexFile().globalIndexMeta() != null);
    }

    private static GlobalIndexIdentifier identifierOf(IndexManifestEntry entry) {
        GlobalIndexMeta globalIndex = entry.indexFile().globalIndexMeta();
        return new GlobalIndexIdentifier(
                entry.indexFile().indexType(),
                globalIndex.indexFieldId(),
                globalIndex.extraFieldIds());
    }

    private static class GlobalIndexIdentifier {

        private final String indexType;
        private final int indexFieldId;
        private final int[] extraFieldIds;

        private GlobalIndexIdentifier(String indexType, int indexFieldId, int[] extraFieldIds) {
            this.indexType = indexType;
            this.indexFieldId = indexFieldId;
            this.extraFieldIds =
                    extraFieldIds == null
                            ? null
                            : Arrays.copyOf(extraFieldIds, extraFieldIds.length);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof GlobalIndexIdentifier)) {
                return false;
            }

            GlobalIndexIdentifier that = (GlobalIndexIdentifier) o;
            return indexFieldId == that.indexFieldId
                    && Objects.equals(indexType, that.indexType)
                    && Arrays.equals(extraFieldIds, that.extraFieldIds);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(indexType, indexFieldId);
            result = 31 * result + Arrays.hashCode(extraFieldIds);
            return result;
        }
    }
}
