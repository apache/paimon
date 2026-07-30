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

package org.apache.paimon.globalindex.sorted;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.globalindex.DataEvolutionGlobalIndexRefreshPlanner;
import org.apache.paimon.globalindex.ScanResult;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RowRangeIndex;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.currentIndexEntries;
import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.unindexedRowRanges;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Scanner for sorted global indexes. */
public class SortedGlobalIndexScanner implements Serializable {

    private static final long serialVersionUID = 1L;
    private final String indexType;
    private final FileStoreTable table;
    private final RowType rowType;
    private final Options options;

    private DataField indexField;

    @Nullable private Snapshot snapshot;

    @Nullable private PartitionPredicate partitionPredicate;

    public SortedGlobalIndexScanner(Table table, String indexType) {
        this(table, indexType, ((FileStoreTable) table).coreOptions().toConfiguration());
    }

    public SortedGlobalIndexScanner(Table table, String indexType, Options options) {
        this.indexType = indexType;
        this.table = (FileStoreTable) table;
        this.rowType = this.table.rowType();
        this.options = options;
    }

    public SortedGlobalIndexScanner withIndexField(String indexField) {
        checkArgument(
                rowType.containsField(indexField),
                "Column '%s' does not exist in table '%s'.",
                indexField,
                table.fullName());
        this.indexField = rowType.getField(indexField);
        return this;
    }

    public SortedGlobalIndexScanner withPartitionPredicate(PartitionPredicate partitionPredicate) {
        this.partitionPredicate = partitionPredicate;
        return this;
    }

    public SortedGlobalIndexScanner withSnapshot(Snapshot snapshot) {
        this.snapshot = snapshot;
        return this;
    }

    public Optional<ScanResult<DataSplit>> scan() {
        SnapshotReader snapshotReader = table.newSnapshotReader();
        if (partitionPredicate != null) {
            snapshotReader = snapshotReader.withPartitionFilter(partitionPredicate);
        }
        Snapshot snapshot =
                this.snapshot != null
                        ? this.snapshot
                        : snapshotReader.snapshotManager().latestSnapshot();
        if (snapshot == null) {
            return Optional.empty();
        }
        snapshotReader = withManifestEntryFilter(snapshotReader.withSnapshot(snapshot));
        Range dataRange = new Range(0, snapshot.nextRowId() - 1);

        return Optional.of(
                new ScanResult<>(
                        snapshot.id(),
                        RowRangeIndex.create(Collections.singletonList(dataRange)),
                        snapshotReader.read().dataSplits(),
                        Collections.emptyList()));
    }

    public Optional<ScanResult<DataSplit>> incrementalScan() {
        SnapshotReader snapshotReader = table.newSnapshotReader();
        if (partitionPredicate != null) {
            snapshotReader = snapshotReader.withPartitionFilter(partitionPredicate);
        }
        Snapshot snapshot =
                this.snapshot != null
                        ? this.snapshot
                        : snapshotReader.snapshotManager().latestSnapshot();
        if (snapshot == null) {
            return Optional.empty();
        }
        snapshotReader = withManifestEntryFilter(snapshotReader.withSnapshot(snapshot));

        Preconditions.checkArgument(indexField != null, "indexField must be set before scan.");
        List<IndexManifestEntry> currentIndexes =
                currentIndexEntries(
                        table,
                        snapshot,
                        indexType,
                        Collections.singletonList(indexField),
                        partitionPredicate);
        List<Range> rangesToBuild = new ArrayList<>(unindexedRowRanges(snapshot, currentIndexes));
        List<IndexManifestEntry> deletedIndexEntries = Collections.emptyList();
        if (detectDataFileChange()) {
            List<ManifestEntry> dataEntries =
                    table.store()
                            .newScan()
                            .withSnapshot(snapshot)
                            .withPartitionFilter(partitionPredicate)
                            .dropStats()
                            .plan()
                            .files();
            deletedIndexEntries =
                    DataEvolutionGlobalIndexRefreshPlanner.findIndexesToRefresh(
                            table.schemaManager(),
                            dataEntries,
                            currentIndexes,
                            Collections.singletonList(indexField));
            for (IndexManifestEntry entry : deletedIndexEntries) {
                rangesToBuild.add(entry.indexFile().globalIndexMeta().rowRange());
            }
        }

        rangesToBuild = Range.sortAndMergeOverlap(rangesToBuild, true);
        if (rangesToBuild.isEmpty()) {
            return Optional.empty();
        }
        snapshotReader = snapshotReader.withRowRanges(rangesToBuild);
        return Optional.of(
                new ScanResult<>(
                        snapshot.id(),
                        RowRangeIndex.create(rangesToBuild),
                        snapshotReader.read().dataSplits(),
                        deletedIndexEntries));
    }

    private boolean detectDataFileChange() {
        return new Options(table.options(), options.toMap())
                .get(CoreOptions.GLOBAL_INDEX_DETECT_DATA_FILE_CHANGE);
    }

    private SnapshotReader withManifestEntryFilter(SnapshotReader snapshotReader) {
        return snapshotReader.withManifestEntryFilter(
                entry ->
                        !isBlobFile(entry.file().fileName())
                                && !isVectorStoreFile(entry.file().fileName()));
    }
}
