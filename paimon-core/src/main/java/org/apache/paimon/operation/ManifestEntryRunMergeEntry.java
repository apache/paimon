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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.manifest.CompactFileIdentifierSet;
import org.apache.paimon.manifest.DeletedRowIdSet;
import org.apache.paimon.manifest.FileEntry.ReusableIdentifier;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ProjectedManifestEntry;
import org.apache.paimon.memory.MemorySegmentUtils;
import org.apache.paimon.utils.ByteArrayKey;
import org.apache.paimon.utils.ByteArrayLookupKey;
import org.apache.paimon.utils.SerializationUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.paimon.utils.Preconditions.checkState;

/** Entry-level state shared by manifest run discovery and merge execution. */
final class ManifestEntryRunMergeEntry {

    private ManifestEntryRunMergeEntry() {}

    static final class Key {

        int partitionId;
        int partitionRank;
        byte kind;
        boolean hasRowId;
        long firstRowId;
        long rangeEnd;
        long reverseSequence;
        byte[] fileNameBytes;
        int fileNameOffset;
        int fileNameLength;

        static Key viewOf(ProjectedManifestEntry entry, PartitionDictionary partitions) {
            Key key = new Key();
            key.replace(entry, partitions);
            return key;
        }

        void replace(ProjectedManifestEntry entry, PartitionDictionary partitions) {
            long firstRowId = entry.file().nonNullFirstRowId();
            this.partitionId = partitions.id(entry.partitionBytes());
            this.partitionRank = partitions.rank(partitionId);
            this.kind = entry.kind().toByteValue();
            this.hasRowId = true;
            this.firstRowId = firstRowId;
            this.rangeEnd = firstRowId + entry.file().rowCount() - 1L;
            this.reverseSequence = Long.MAX_VALUE - entry.file().maxSequenceNumber();
            this.fileNameBytes = entry.file().fileNameBinary().toBytes();
            this.fileNameOffset = 0;
            this.fileNameLength = fileNameBytes.length;
        }

        void replace(GenericRow record, PartitionDictionary partitions) {
            InternalRow file = file(record);
            checkState(
                    !file.isNullAt(ManifestEntryRunMerge.FIRST_ROW_ID),
                    "First row id should not be null.");
            this.partitionId = partitions.id(record.getBinary(ManifestEntryRunMerge.PARTITION));
            this.partitionRank = partitions.rank(partitionId);
            this.kind = record.getByte(ManifestEntryRunMerge.KIND);
            this.hasRowId = true;
            this.firstRowId = file.getLong(ManifestEntryRunMerge.FIRST_ROW_ID);
            this.rangeEnd = firstRowId + file.getLong(ManifestEntryRunMerge.ROW_COUNT) - 1L;
            this.reverseSequence =
                    Long.MAX_VALUE - file.getLong(ManifestEntryRunMerge.MAX_SEQUENCE_NUMBER);
            BinaryString fileName = file.getString(ManifestEntryRunMerge.FILE_NAME);
            this.fileNameBytes =
                    MemorySegmentUtils.copyToBytes(
                            fileName.getSegments(),
                            fileName.getOffset(),
                            fileName.getSizeInBytes());
            this.fileNameOffset = 0;
            this.fileNameLength = fileNameBytes.length;
        }

        void replaceForCompaction(GenericRow record) {
            InternalRow file = file(record);
            this.kind = record.getByte(ManifestEntryRunMerge.KIND);
            this.hasRowId = !file.isNullAt(ManifestEntryRunMerge.FIRST_ROW_ID);
            if (hasRowId) {
                this.firstRowId = file.getLong(ManifestEntryRunMerge.FIRST_ROW_ID);
                this.rangeEnd = firstRowId + file.getLong(ManifestEntryRunMerge.ROW_COUNT) - 1L;
            }
        }

        void copyFrom(Key key) {
            this.partitionId = key.partitionId;
            this.partitionRank = key.partitionRank;
            this.kind = key.kind;
            this.hasRowId = key.hasRowId;
            this.firstRowId = key.firstRowId;
            this.rangeEnd = key.rangeEnd;
            this.reverseSequence = key.reverseSequence;
            this.fileNameBytes = key.fileNameBytes;
            this.fileNameOffset = key.fileNameOffset;
            this.fileNameLength = key.fileNameLength;
        }

        Key stableCopy() {
            Key copy = new Key();
            copy.copyFrom(this);
            copy.fileNameBytes =
                    Arrays.copyOfRange(
                            fileNameBytes, fileNameOffset, fileNameOffset + fileNameLength);
            copy.fileNameOffset = 0;
            return copy;
        }

        void clear() {
            fileNameBytes = null;
        }
    }

    /** Interns variable-width partition bytes once and assigns comparator-compatible ranks. */
    static final class PartitionDictionary {

        final ManifestFileSorter.RowIdEntrySortKey sortKey;
        final Map<ByteArrayKey, Integer> ids = new ConcurrentHashMap<>();
        final ThreadLocal<ByteArrayLookupKey> lookup =
                ThreadLocal.withInitial(ByteArrayLookupKey::new);
        volatile BinaryRow[] partitions = new BinaryRow[16];
        int partitionCount;
        int[] ranks;

        PartitionDictionary(ManifestFileSorter.RowIdEntrySortKey sortKey) {
            this.sortKey = sortKey;
        }

        PartitionDictionary() {
            this.sortKey = null;
        }

        int id(byte[] bytes) {
            return id(bytes, 0, bytes.length);
        }

        int id(byte[] bytes, int offset, int length) {
            ByteArrayLookupKey lookupKey = lookup.get();
            lookupKey.reset(bytes, offset, length);
            try {
                Integer existing = ids.get(lookupKey);
                if (existing != null) {
                    return existing;
                }
                synchronized (this) {
                    existing = ids.get(lookupKey);
                    if (existing != null) {
                        return existing;
                    }
                    checkState(ranks == null, "Full manifest scan found an unknown partition.");
                    byte[] canonical = Arrays.copyOfRange(bytes, offset, offset + length);
                    int id = partitionCount;
                    if (id == partitions.length) {
                        partitions = Arrays.copyOf(partitions, partitions.length << 1);
                    }
                    partitions[id] = SerializationUtils.deserializeBinaryRow(canonical);
                    ids.put(new ByteArrayKey(canonical), id);
                    partitionCount = id + 1;
                    return id;
                }
            } finally {
                lookupKey.clear();
            }
        }

        int compareIds(int left, int right) {
            checkState(sortKey != null, "Partition dictionary has no sort key.");
            return sortKey.comparePartitions(partitions[left], partitions[right]);
        }

        void finish() {
            List<Integer> order = new ArrayList<>(partitionCount);
            for (int id = 0; id < partitionCount; id++) {
                order.add(id);
            }
            order.sort((left, right) -> compareIds(left, right));
            ranks = new int[partitionCount];
            int rank = 0;
            for (int position = 0; position < order.size(); position++) {
                if (position > 0 && compareIds(order.get(position - 1), order.get(position)) != 0) {
                    rank++;
                }
                ranks[order.get(position)] = rank;
            }
        }

        int rank(int id) {
            return ranks == null ? 0 : ranks[id];
        }

        BinaryRow partition(int id) {
            return partitions[id];
        }
    }

    static class Filter {

        final CompactFileIdentifierSet deletedIdentifiers;
        final DeletedRowIdSet deletedRowIds;
        final boolean useRowIdFilter;
        final ThreadLocal<IdentifierEncoder> identifier =
                ThreadLocal.withInitial(IdentifierEncoder::new);

        Filter(
                CompactFileIdentifierSet deletedIdentifiers,
                DeletedRowIdSet deletedRowIds,
                boolean useRowIdFilter) {
            this.deletedIdentifiers = deletedIdentifiers;
            this.deletedRowIds = deletedRowIds;
            this.useRowIdFilter = useRowIdFilter;
        }

        boolean include(ProjectedManifestEntry entry) {
            return entry.isAdd() && !deletedIdentifiers.contains(entry);
        }

        boolean include(GenericRow record, Key key) {
            return key.kind == FileKind.ADD.toByteValue() && !isDeleted(record, key);
        }

        boolean copyable(GenericRow record, Key key) {
            return include(record, key);
        }

        void observe(GenericRow record, Key key) {}

        boolean copyableAfterDiscovery(long minRowId, long maxRowId) {
            return true;
        }

        ReusableIdentifier identifier(GenericRow record) {
            return identifier.get().replace(record);
        }

        boolean isDeleted(GenericRow record, Key key) {
            // RowID is only a cheap negative filter. The complete identifier remains the
            // authoritative match, and is also sufficient for manifests which predate RowID.
            if (useRowIdFilter) {
                checkState(key.hasRowId, "First row id should not be null.");
                if (!deletedRowIds.contains(key.firstRowId)) {
                    return false;
                }
            }
            return deletedIdentifiers.contains(identifier(record));
        }

        static final class Minor extends Filter {

            Minor(
                    CompactFileIdentifierSet deletedIdentifiers,
                    DeletedRowIdSet deletedRowIds,
                    boolean useRowIdFilter) {
                super(deletedIdentifiers, deletedRowIds, useRowIdFilter);
            }

            @Override
            boolean include(ProjectedManifestEntry entry) {
                return true;
            }

            @Override
            boolean include(GenericRow record, Key key) {
                return true;
            }

            @Override
            boolean copyable(GenericRow record, Key key) {
                return key.kind == FileKind.ADD.toByteValue();
            }

            @Override
            void observe(GenericRow record, Key key) {
                if (key.kind != FileKind.DELETE.toByteValue()) {
                    return;
                }
                ReusableIdentifier reusable = identifier(record);
                synchronized (this) {
                    deletedIdentifiers.add(reusable);
                    if (useRowIdFilter) {
                        checkState(key.hasRowId, "First row id should not be null.");
                        deletedRowIds.add(key.firstRowId);
                    }
                }
            }

            @Override
            boolean copyableAfterDiscovery(long minRowId, long maxRowId) {
                // A DELETE preserves the deleted ADD's globally unique first RowID. A range hit may
                // be a false positive and only disables block copying; a miss proves the block has
                // no deleted ADD.
                return useRowIdFilter && !deletedRowIds.intersects(minRowId, maxRowId);
            }
        }

        private static final class IdentifierEncoder {

            final ProjectedManifestEntry entry =
                    ProjectedManifestEntry.Projection.create(ManifestEntryRunMerge.ENTRY_LAYOUT)
                            .createEntry();
            final ReusableIdentifier identifier = new ReusableIdentifier();

            ReusableIdentifier replace(GenericRow record) {
                return identifier.replaceWithPartition(entry.replace(record));
            }
        }
    }

    static InternalRow file(GenericRow record) {
        return record.getRow(ManifestEntryRunMerge.FILE, ManifestEntryRunMerge.FILE_FIELD_COUNT);
    }
}
