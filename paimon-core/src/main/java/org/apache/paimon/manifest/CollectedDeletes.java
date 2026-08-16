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

package org.apache.paimon.manifest;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.ProjectedDataFileMeta;
import org.apache.paimon.manifest.FileEntry.ReusableIdentifier;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkState;

/** DELETE identifiers and optional RowID and partition indexes collected for manifest merging. */
public final class CollectedDeletes {

    private final CompactFileIdentifierSet identifiers = new CompactFileIdentifierSet();
    private final DeletedRowIdSet rowIds = new DeletedRowIdSet();
    private Set<BinaryRow> partitions = new HashSet<>();
    private final boolean useRowIdFilter;
    private boolean immutable;

    public CollectedDeletes(boolean useRowIdFilter) {
        this.useRowIdFilter = useRowIdFilter;
    }

    public void add(
            ProjectedManifestEntry entry, boolean collectRowIds, boolean collectPartitions) {
        checkState(!immutable, "Cannot modify an immutable DELETE collection.");
        identifiers.add(entry);
        if (collectPartitions) {
            partitions.add(entry.partition().copy());
        }
        if (collectRowIds) {
            rowIds.add(entry.file().nonNullFirstRowId());
        }
    }

    public void combine(CollectedDeletes other) {
        checkState(!immutable, "Cannot modify an immutable DELETE collection.");
        checkState(
                useRowIdFilter == other.useRowIdFilter,
                "Cannot combine DELETE collections with different RowID modes.");
        identifiers.addAll(other.identifiers);
        rowIds.addAll(other.rowIds);
        partitions.addAll(other.partitions);
    }

    public CollectedDeletes toImmutable() {
        checkState(!immutable, "Cannot modify an immutable DELETE collection.");
        if (useRowIdFilter) {
            rowIds.prepareRangeIndex();
        }
        partitions = Collections.unmodifiableSet(partitions);
        immutable = true;
        return this;
    }

    public boolean isEmpty() {
        return identifiers.isEmpty();
    }

    public Set<BinaryRow> partitions() {
        return partitions;
    }

    public boolean useRowIdFilter() {
        return useRowIdFilter;
    }

    public boolean isDeleted(ProjectedManifestEntry entry, ReusableIdentifier reusableIdentifier) {
        if (useRowIdFilter) {
            ProjectedDataFileMeta file = entry.file();
            checkState(file.hasFirstRowId(), "First row id should not be null.");
            if (!rowIds.contains(file.nonNullFirstRowId())) {
                return false;
            }
        }
        return identifiers.contains(reusableIdentifier.replaceWithPartition(entry));
    }

    public boolean copyable(
            ProjectedManifestEntry entry,
            ReusableIdentifier reusableIdentifier,
            boolean deferDeletedAddCheck) {
        return entry.isAdd() && (deferDeletedAddCheck || !isDeleted(entry, reusableIdentifier));
    }

    public boolean intersectsRowIds(long minRowId, long maxRowId) {
        return rowIds.intersects(minRowId, maxRowId);
    }

    public void release() {
        identifiers.release();
        rowIds.releaseRangeIndex();
    }
}
