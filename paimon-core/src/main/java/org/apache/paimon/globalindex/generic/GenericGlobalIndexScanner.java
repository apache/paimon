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

package org.apache.paimon.globalindex.generic;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.globalindex.DataEvolutionGlobalIndexRefreshPlanner;
import org.apache.paimon.globalindex.ScanResult;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RowRangeIndex;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.currentIndexEntries;
import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.unindexedRowRanges;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Scanner for generic (non-btree) global index. */
public class GenericGlobalIndexScanner implements Serializable {

    private static final long serialVersionUID = 1L;

    protected final FileStoreTable table;

    @Nullable protected PartitionPredicate partitionPredicate;
    @Nullable private String indexType;
    private List<DataField> indexFields = Collections.emptyList();
    private Options options = new Options();

    public GenericGlobalIndexScanner(FileStoreTable table) {
        this.table = table;
    }

    public GenericGlobalIndexScanner withPartitionPredicate(PartitionPredicate partitionPredicate) {
        this.partitionPredicate = partitionPredicate;
        return this;
    }

    public GenericGlobalIndexScanner withIndex(
            String indexType, List<String> indexColumns, Options options) {
        checkArgument(!indexColumns.isEmpty(), "Index columns must not be empty.");
        List<DataField> indexFields = new ArrayList<>(indexColumns.size());
        for (String indexColumn : indexColumns) {
            checkArgument(
                    table.rowType().containsField(indexColumn),
                    "Column '%s' does not exist in table '%s'.",
                    indexColumn,
                    table.fullName());
            indexFields.add(table.rowType().getField(indexColumn));
        }
        this.indexType = indexType;
        this.indexFields = Collections.unmodifiableList(indexFields);
        this.options = options;
        return this;
    }

    public FileStoreTable table() {
        return table;
    }

    /**
     * Scans manifest entries to determine which files need to be indexed.
     *
     * @return scan result containing manifest entries to build index from
     */
    public Optional<ScanResult<ManifestEntry>> scan() {
        checkArgument(
                table.coreOptions().bucket() == -1,
                "Generic global index only supports unaware-bucket tables (bucket = -1), "
                        + "but table '%s' has bucket = %d.",
                table.name(),
                table.coreOptions().bucket());
        Snapshot scanSnapshot = table.snapshotManager().latestSnapshot();
        if (scanSnapshot == null) {
            return Optional.empty();
        }

        Long nextRowId = scanSnapshot.nextRowId();
        if (nextRowId == null || nextRowId <= 0) {
            return Optional.empty();
        }
        Range dataRange = new Range(0, nextRowId - 1);

        List<ManifestEntry> entries =
                table.store()
                        .newScan()
                        .withSnapshot(scanSnapshot)
                        .withPartitionFilter(partitionPredicate)
                        .dropStats()
                        .plan()
                        .files();
        return Optional.of(
                new ScanResult<>(
                        scanSnapshot.id(),
                        RowRangeIndex.create(Collections.singletonList(dataRange)),
                        entries,
                        Collections.emptyList()));
    }

    public Optional<ScanResult<ManifestEntry>> incrementalScan() {
        Optional<ScanResult<ManifestEntry>> optionalScanResult = scan();
        if (!optionalScanResult.isPresent()) {
            return Optional.empty();
        }

        checkArgument(indexType != null, "Index type must be set before incremental scan.");
        checkArgument(!indexFields.isEmpty(), "Index fields must be set before incremental scan.");

        ScanResult<ManifestEntry> scanResult = optionalScanResult.get();
        Snapshot scanSnapshot = table.snapshotManager().snapshot(scanResult.scanSnapshotId());
        List<IndexManifestEntry> currentIndexes =
                currentIndexEntries(
                        table, scanSnapshot, indexType, indexFields, partitionPredicate);
        List<Range> rangesToBuild =
                new ArrayList<>(unindexedRowRanges(scanSnapshot, currentIndexes));
        List<IndexManifestEntry> deletedIndexEntries = Collections.emptyList();
        Options mergedOptions = new Options(table.options(), options.toMap());
        if (mergedOptions.get(CoreOptions.GLOBAL_INDEX_DETECT_DATA_FILE_CHANGE)) {
            deletedIndexEntries =
                    DataEvolutionGlobalIndexRefreshPlanner.findIndexesToRefresh(
                            table.schemaManager(),
                            scanResult.entries(),
                            currentIndexes,
                            indexFields);
            for (IndexManifestEntry entry : deletedIndexEntries) {
                rangesToBuild.add(entry.indexFile().globalIndexMeta().rowRange());
            }
        }

        rangesToBuild = Range.sortAndMergeOverlap(rangesToBuild, true);
        if (rangesToBuild.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(
                new ScanResult<>(
                        scanResult.scanSnapshotId(),
                        RowRangeIndex.create(rangesToBuild),
                        scanResult.entries(),
                        deletedIndexEntries));
    }
}
