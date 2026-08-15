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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.manifest.CompactFileIdentifierSet;
import org.apache.paimon.manifest.DeletedRowIdSet;
import org.apache.paimon.manifest.FileEntry.ReusableIdentifier;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ProjectedManifestEntry;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentUtils;

import static org.apache.paimon.utils.Preconditions.checkState;

/** Entry-level state shared by manifest run discovery and merge execution. */
final class ManifestEntryRunMergeEntry {

    private ManifestEntryRunMergeEntry() {}

    static final class Key {

        int partitionId;
        int partitionRank;
        byte kind;
        long firstRowId;
        long rangeEnd;
        long reverseSequence;
        MemorySegment[] fileNameSegments;
        int fileNameOffset;
        int fileNameLength;
        byte[] ownedFileNameBytes;
        MemorySegment[] ownedFileNameSegments;

        static Key viewOf(
                ProjectedManifestEntry entry, ManifestEntryRunMergePartitionDictionary partitions) {
            Key key = new Key();
            key.replace(entry, partitions);
            return key;
        }

        void replace(
                ProjectedManifestEntry entry, ManifestEntryRunMergePartitionDictionary partitions) {
            long firstRowId = entry.file().nonNullFirstRowId();
            this.partitionId = partitions.id(entry.partitionBytes());
            this.partitionRank = partitions.rank(partitionId);
            this.kind = entry.kind().toByteValue();
            this.firstRowId = firstRowId;
            this.rangeEnd = firstRowId + entry.file().rowCount() - 1L;
            this.reverseSequence = Long.MAX_VALUE - entry.file().maxSequenceNumber();
            BinaryString fileName = entry.file().fileNameBinary();
            this.fileNameSegments = fileName.getSegments();
            this.fileNameOffset = fileName.getOffset();
            this.fileNameLength = fileName.getSizeInBytes();
        }

        void copyFrom(Key key) {
            this.partitionId = key.partitionId;
            this.partitionRank = key.partitionRank;
            this.kind = key.kind;
            this.firstRowId = key.firstRowId;
            this.rangeEnd = key.rangeEnd;
            this.reverseSequence = key.reverseSequence;
            ensureFileNameCapacity(key.fileNameLength);
            MemorySegmentUtils.copyToBytes(
                    key.fileNameSegments,
                    key.fileNameOffset,
                    ownedFileNameBytes,
                    0,
                    key.fileNameLength);
            this.fileNameSegments = ownedFileNameSegments;
            this.fileNameOffset = 0;
            this.fileNameLength = key.fileNameLength;
        }

        Key stableCopy() {
            Key copy = new Key();
            copy.copyFrom(this);
            return copy;
        }

        private void ensureFileNameCapacity(int length) {
            if (ownedFileNameBytes == null || ownedFileNameBytes.length < length) {
                ownedFileNameBytes = new byte[length];
                ownedFileNameSegments =
                        new MemorySegment[] {MemorySegment.wrap(ownedFileNameBytes)};
            }
        }

        void clear() {
            fileNameSegments = null;
            ownedFileNameBytes = null;
            ownedFileNameSegments = null;
        }
    }

    static class Filter {

        final CompactFileIdentifierSet deletedIdentifiers;
        final DeletedRowIdSet deletedRowIds;
        final boolean useRowIdFilter;
        final ThreadLocal<ReusableIdentifier> identifier =
                ThreadLocal.withInitial(ReusableIdentifier::new);

        Filter(
                CompactFileIdentifierSet deletedIdentifiers,
                DeletedRowIdSet deletedRowIds,
                boolean useRowIdFilter) {
            this.deletedIdentifiers = deletedIdentifiers;
            this.deletedRowIds = deletedRowIds;
            this.useRowIdFilter = useRowIdFilter;
        }

        boolean include(ProjectedManifestEntry entry) {
            return entry.isAdd() && !deletedIdentifiers.contains(identifier(entry));
        }

        boolean include(ProjectedManifestEntry entry, Key key) {
            return key.kind == FileKind.ADD.toByteValue() && !isDeleted(entry, key);
        }

        boolean copyable(ProjectedManifestEntry entry, Key key) {
            return include(entry, key);
        }

        void observe(ProjectedManifestEntry entry, Key key) {}

        boolean copyableAfterDiscovery(long minRowId, long maxRowId) {
            return true;
        }

        Filter forDiscovery() {
            return this;
        }

        void combine(Filter other) {
            checkState(other == this, "Immutable manifest filter cannot collect local DELETEs.");
        }

        ReusableIdentifier identifier(ProjectedManifestEntry entry) {
            return identifier.get().replaceWithPartition(entry);
        }

        void releaseIdentifier() {
            ReusableIdentifier reusableIdentifier = identifier.get();
            reusableIdentifier.release();
            identifier.remove();
        }

        boolean isDeleted(ProjectedManifestEntry entry, Key key) {
            // RowID is only a cheap negative filter. The complete identifier remains the
            // authoritative match, and is also sufficient for manifests which predate RowID.
            if (useRowIdFilter) {
                if (!deletedRowIds.contains(key.firstRowId)) {
                    return false;
                }
            }
            return deletedIdentifiers.contains(identifier(entry));
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
            boolean include(ProjectedManifestEntry entry, Key key) {
                return true;
            }

            @Override
            boolean copyable(ProjectedManifestEntry entry, Key key) {
                return key.kind == FileKind.ADD.toByteValue();
            }

            @Override
            void observe(ProjectedManifestEntry entry, Key key) {
                if (key.kind != FileKind.DELETE.toByteValue()) {
                    return;
                }
                ReusableIdentifier reusable = identifier(entry);
                deletedIdentifiers.add(reusable);
                if (useRowIdFilter) {
                    deletedRowIds.add(key.firstRowId);
                }
            }

            @Override
            Filter forDiscovery() {
                return new Minor(
                        new CompactFileIdentifierSet(), new DeletedRowIdSet(), useRowIdFilter);
            }

            @Override
            void combine(Filter other) {
                checkState(other instanceof Minor, "Cannot combine incompatible manifest filters.");
                deletedIdentifiers.addAll(other.deletedIdentifiers);
                deletedRowIds.addAll(other.deletedRowIds);
                other.deletedIdentifiers.release();
                other.deletedRowIds.releaseRangeIndex();
            }

            @Override
            boolean copyableAfterDiscovery(long minRowId, long maxRowId) {
                // A DELETE preserves the deleted ADD's globally unique first RowID. A range hit may
                // be a false positive and only disables block copying; a miss proves the block has
                // no deleted ADD.
                return useRowIdFilter && !deletedRowIds.intersects(minRowId, maxRowId);
            }
        }
    }
}
