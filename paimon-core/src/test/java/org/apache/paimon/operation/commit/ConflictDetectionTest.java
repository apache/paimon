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

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.manifest.SimpleFileEntryWithDV;
import org.apache.paimon.operation.commit.RetryCommitResult.CommitFailRetryResult;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.manifest.FileKind.ADD;
import static org.apache.paimon.manifest.FileKind.DELETE;
import static org.apache.paimon.operation.commit.ConflictDetection.buildBaseEntriesWithDV;
import static org.apache.paimon.operation.commit.ConflictDetection.buildDeltaEntriesWithDV;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ConflictDetectionTest {

    @Test
    void testCreateConflictDetectionByTableType() {
        ConflictDetection append = createConflictDetection(null, false, false);
        assertThat(append).isInstanceOf(AppendConflictDetection.class);
        assertThat(append.keyComparator()).isNull();
        assertThat(createConflictDetection(null, false, true))
                .isInstanceOf(PrimaryKeyConflictDetection.class);
        assertThat(createConflictDetection(null, true, false))
                .isInstanceOf(DataEvolutionConflictDetection.class);
        // Data Evolution takes precedence even if a key comparator is supplied.
        assertThat(createConflictDetection(null, true, true))
                .isInstanceOf(DataEvolutionConflictDetection.class);
    }

    @Test
    void testPrimaryKeyClusteringOverrideSkipsLsmConflictCheck() {
        ConflictDetection detection = createConflictDetection(null, false, true, true, null);

        assertThat(
                        detection.checkConflicts(
                                snapshot(1),
                                Collections.singletonList(createLevelFileEntry("base", 1)),
                                Collections.singletonList(createLevelFileEntry("delta", 1)),
                                Collections.emptyList(),
                                null,
                                Snapshot.CommitKind.COMPACT))
                .isEmpty();
    }

    @Test
    void testDataEvolutionCompactScansChangedRowRanges() {
        CommitScanner scanner = mock(CommitScanner.class);
        DataEvolutionConflictDetection detection =
                (DataEvolutionConflictDetection) createConflictDetection(scanner, true, false);
        Snapshot snapshot = snapshot(1);
        BinaryRow partition = BinaryRow.singleColumn(1);
        List<BinaryRow> changedPartitions = Collections.singletonList(partition);
        Range changedRange = new Range(10, 19);

        ManifestEntry delta = mock(ManifestEntry.class);
        DataFileMeta dataFile = mock(DataFileMeta.class);
        when(delta.kind()).thenReturn(ADD);
        when(delta.file()).thenReturn(dataFile);
        when(dataFile.firstRowId()).thenReturn(10L);
        when(dataFile.nonNullRowIdRange()).thenReturn(changedRange);
        when(scanner.readAllEntriesFromChangedRowRanges(
                        snapshot, changedPartitions, Collections.singletonList(changedRange)))
                .thenReturn(Collections.emptyList());

        assertThat(
                        detection.scanBaseDataFiles(
                                snapshot,
                                changedPartitions,
                                Collections.singletonList(delta),
                                Collections.emptyList(),
                                Snapshot.CommitKind.COMPACT,
                                null,
                                false))
                .isEmpty();
        verify(scanner)
                .readAllEntriesFromChangedRowRanges(
                        snapshot, changedPartitions, Collections.singletonList(changedRange));
    }

    @Test
    void testDataEvolutionCompactSupplementsReferencedDataFiles() {
        CommitScanner scanner = mock(CommitScanner.class);
        DataEvolutionConflictDetection detection =
                (DataEvolutionConflictDetection) createConflictDetection(scanner, true, false);
        Snapshot snapshot = snapshot(1);
        BinaryRow partition = BinaryRow.singleColumn(1);
        List<BinaryRow> changedPartitions = Collections.singletonList(partition);
        Range changedRange = new Range(10, 19);

        ManifestEntry added = manifestEntry(ADD, "added", 10L, changedRange);
        ManifestEntry deleted = manifestEntry(DELETE, "legacy", null, null);
        SimpleFileEntry rangeBase =
                createFileEntryWithRowId("range-base", ADD, partition, 0, 10L, 10L);
        SimpleFileEntry legacyBase = createFileEntry("legacy", ADD);
        SimpleFileEntry dvBase = createFileEntry("dv-base", ADD);
        Set<String> referencedFiles = new HashSet<>(Arrays.asList("legacy", "dv-base"));
        when(scanner.readAllEntriesFromChangedRowRanges(
                        snapshot, changedPartitions, Collections.singletonList(changedRange)))
                .thenReturn(Collections.singletonList(rangeBase));
        when(scanner.readAllEntriesFromDataFiles(snapshot, changedPartitions, referencedFiles))
                .thenReturn(Arrays.asList(legacyBase, dvBase));

        assertThat(
                        detection.scanBaseDataFiles(
                                snapshot,
                                changedPartitions,
                                Arrays.asList(added, deleted),
                                Collections.singletonList(
                                        createDvIndexEntry(
                                                "dv", ADD, Collections.singletonList("dv-base"))),
                                Snapshot.CommitKind.COMPACT,
                                null,
                                false))
                .containsExactly(rangeBase, legacyBase, dvBase);
        verify(scanner).readAllEntriesFromDataFiles(snapshot, changedPartitions, referencedFiles);
    }

    @Test
    void testDataEvolutionCompactWithoutRowRangesReusesRetryScan() {
        CommitScanner scanner = mock(CommitScanner.class);
        DataEvolutionConflictDetection detection =
                (DataEvolutionConflictDetection) createConflictDetection(scanner, true, false);
        Snapshot previousSnapshot = snapshot(1);
        Snapshot latestSnapshot = snapshot(2);
        List<BinaryRow> changedPartitions = Collections.singletonList(BinaryRow.singleColumn(1));
        SimpleFileEntry oldBase = createFileEntry("old", ADD);
        SimpleFileEntry removedBase = createFileEntry("old", DELETE);
        SimpleFileEntry newBase = createFileEntry("new", ADD);
        List<SimpleFileEntry> cachedBase = Collections.singletonList(oldBase);
        CommitFailRetryResult previousAttempt = commitFailRetryResult(previousSnapshot, cachedBase);
        when(scanner.readIncrementalChanges(previousSnapshot, latestSnapshot, changedPartitions))
                .thenReturn(Arrays.asList(removedBase, newBase));

        assertThat(
                        detection.scanBaseDataFiles(
                                latestSnapshot,
                                changedPartitions,
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Snapshot.CommitKind.COMPACT,
                                previousAttempt,
                                false))
                .containsExactly(newBase);
        assertThat(cachedBase).containsExactly(oldBase);
        verify(scanner, never())
                .readAllEntriesFromChangedPartitions(latestSnapshot, changedPartitions);
    }

    @Test
    void testRetryFallsBackWhenCachedScanCannotBeReused() {
        CommitScanner scanner = mock(CommitScanner.class);
        ConflictDetection detection = createConflictDetection(scanner, false, false);
        Snapshot previousSnapshot = snapshot(1);
        Snapshot latestSnapshot = snapshot(2);
        List<BinaryRow> changedPartitions = Collections.singletonList(BinaryRow.singleColumn(1));
        List<SimpleFileEntry> cachedBase =
                Collections.singletonList(createFileEntry("cached", ADD));
        List<SimpleFileEntry> expected = Collections.singletonList(createFileEntry("latest", ADD));
        when(scanner.readAllEntriesFromChangedPartitions(latestSnapshot, changedPartitions))
                .thenReturn(expected);

        assertThat(
                        detection.scanBaseDataFiles(
                                latestSnapshot,
                                changedPartitions,
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Snapshot.CommitKind.APPEND,
                                commitFailRetryResult(null, cachedBase),
                                false))
                .isSameAs(expected);
        assertThat(
                        detection.scanBaseDataFiles(
                                latestSnapshot,
                                changedPartitions,
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Snapshot.CommitKind.APPEND,
                                commitFailRetryResult(previousSnapshot, null),
                                false))
                .isSameAs(expected);
        assertThat(
                        detection.scanBaseDataFiles(
                                latestSnapshot,
                                changedPartitions,
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Snapshot.CommitKind.APPEND,
                                commitFailRetryResult(previousSnapshot, cachedBase),
                                true))
                .isSameAs(expected);
        verify(scanner, times(3))
                .readAllEntriesFromChangedPartitions(latestSnapshot, changedPartitions);
        verify(scanner, never())
                .readIncrementalChanges(previousSnapshot, latestSnapshot, changedPartitions);
    }

    @Test
    void testDataEvolutionOverwriteScansDeletedFileRowRange() {
        CommitScanner scanner = mock(CommitScanner.class);
        DataEvolutionConflictDetection detection =
                (DataEvolutionConflictDetection) createConflictDetection(scanner, true, false);
        Snapshot snapshot = snapshot(1);
        BinaryRow partition = BinaryRow.singleColumn(1);
        List<BinaryRow> changedPartitions = Collections.singletonList(partition);
        Range changedRange = new Range(10, 19);

        ManifestEntry deleted = manifestEntry(DELETE, "deleted", 10L, changedRange);
        SimpleFileEntry base = createFileEntryWithRowId("deleted", ADD, partition, 0, 10L, 10L);
        when(scanner.readAllEntriesFromChangedRowRanges(
                        snapshot, changedPartitions, Collections.singletonList(changedRange)))
                .thenReturn(Collections.singletonList(base));

        assertThat(
                        detection.scanBaseDataFiles(
                                snapshot,
                                changedPartitions,
                                Collections.singletonList(deleted),
                                Collections.emptyList(),
                                Snapshot.CommitKind.OVERWRITE,
                                null,
                                false))
                .containsExactly(base);
        verify(scanner, never())
                .readAllEntriesFromDataFiles(
                        snapshot, changedPartitions, Collections.singleton("deleted"));
        verify(scanner, never()).readAllEntriesFromChangedPartitions(snapshot, changedPartitions);
    }

    @Test
    void testDataEvolutionOverwriteScansDeletionVectorDataFile() {
        CommitScanner scanner = mock(CommitScanner.class);
        DataEvolutionConflictDetection detection =
                (DataEvolutionConflictDetection) createConflictDetection(scanner, true, false);
        Snapshot snapshot = snapshot(1);
        List<BinaryRow> changedPartitions = Collections.singletonList(BinaryRow.singleColumn(1));
        SimpleFileEntry base = createFileEntry("base", ADD);
        when(scanner.readAllEntriesFromDataFiles(
                        snapshot, changedPartitions, Collections.singleton("base")))
                .thenReturn(Collections.singletonList(base));

        assertThat(
                        detection.scanBaseDataFiles(
                                snapshot,
                                changedPartitions,
                                Collections.emptyList(),
                                Collections.singletonList(
                                        createDvIndexEntry(
                                                "dv", ADD, Collections.singletonList("base"))),
                                Snapshot.CommitKind.OVERWRITE,
                                null,
                                false))
                .containsExactly(base);
        verify(scanner, never()).readAllEntriesFromChangedPartitions(snapshot, changedPartitions);
    }

    @Test
    void testDataEvolutionOverwriteSupplementsLegacyDeletedFile() {
        CommitScanner scanner = mock(CommitScanner.class);
        DataEvolutionConflictDetection detection =
                (DataEvolutionConflictDetection) createConflictDetection(scanner, true, false);
        Snapshot snapshot = snapshot(1);
        BinaryRow partition = BinaryRow.singleColumn(1);
        List<BinaryRow> changedPartitions = Collections.singletonList(partition);
        Range changedRange = new Range(10, 19);

        ManifestEntry added = manifestEntry(ADD, "added", 10L, changedRange);
        ManifestEntry deleted = manifestEntry(DELETE, "legacy", null, null);
        SimpleFileEntry rangeBase =
                createFileEntryWithRowId("range-base", ADD, partition, 0, 10L, 10L);
        SimpleFileEntry legacyBase = createFileEntry("legacy", ADD);
        when(scanner.readAllEntriesFromChangedRowRanges(
                        snapshot, changedPartitions, Collections.singletonList(changedRange)))
                .thenReturn(Collections.singletonList(rangeBase));
        when(scanner.readAllEntriesFromDataFiles(
                        snapshot, changedPartitions, Collections.singleton("legacy")))
                .thenReturn(Collections.singletonList(legacyBase));

        assertThat(
                        detection.scanBaseDataFiles(
                                snapshot,
                                changedPartitions,
                                Arrays.asList(added, deleted),
                                Collections.emptyList(),
                                Snapshot.CommitKind.OVERWRITE,
                                null,
                                false))
                .containsExactly(rangeBase, legacyBase);
        verify(scanner, never()).readAllEntriesFromChangedPartitions(snapshot, changedPartitions);
    }

    @Test
    void testDataEvolutionOverwriteFallsBackWithoutSelectors() {
        CommitScanner scanner = mock(CommitScanner.class);
        DataEvolutionConflictDetection detection =
                (DataEvolutionConflictDetection) createConflictDetection(scanner, true, false);
        Snapshot snapshot = snapshot(1);
        List<BinaryRow> changedPartitions = Collections.singletonList(BinaryRow.singleColumn(1));
        List<SimpleFileEntry> expected = Collections.singletonList(createFileEntry("base", ADD));
        when(scanner.readAllEntriesFromChangedPartitions(snapshot, changedPartitions))
                .thenReturn(expected);

        assertThat(
                        detection.scanBaseDataFiles(
                                snapshot,
                                changedPartitions,
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Snapshot.CommitKind.OVERWRITE,
                                null,
                                false))
                .isSameAs(expected);
    }

    @Test
    void testDataEvolutionAppendUsesDefaultPartitionScan() {
        CommitScanner scanner = mock(CommitScanner.class);
        DataEvolutionConflictDetection detection =
                (DataEvolutionConflictDetection) createConflictDetection(scanner, true, false);
        Snapshot snapshot = snapshot(1);
        List<BinaryRow> changedPartitions = Collections.singletonList(BinaryRow.singleColumn(1));
        List<SimpleFileEntry> expected = Collections.singletonList(createFileEntry("base", ADD));
        when(scanner.readAllEntriesFromChangedPartitions(snapshot, changedPartitions))
                .thenReturn(expected);

        assertThat(
                        detection.scanBaseDataFiles(
                                snapshot,
                                changedPartitions,
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Snapshot.CommitKind.APPEND,
                                null,
                                false))
                .isSameAs(expected);
    }

    @Test
    void testAppendScansChangedPartitions() {
        CommitScanner scanner = mock(CommitScanner.class);
        ConflictDetection detection = createConflictDetection(scanner, false, false);
        Snapshot snapshot = snapshot(1);
        List<BinaryRow> changedPartitions = Collections.singletonList(BinaryRow.singleColumn(1));
        List<SimpleFileEntry> expected = Collections.singletonList(createFileEntry("base", ADD));
        when(scanner.readAllEntriesFromChangedPartitions(snapshot, changedPartitions))
                .thenReturn(expected);

        assertThat(
                        detection.scanBaseDataFiles(
                                snapshot,
                                changedPartitions,
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Snapshot.CommitKind.APPEND,
                                null,
                                false))
                .isSameAs(expected);
        verify(scanner).readAllEntriesFromChangedPartitions(snapshot, changedPartitions);
    }

    @Test
    void testAppendSkipsDataEvolutionConflictChecks() {
        ConflictDetection detection = createConflictDetection(null, false, false);

        Optional<RuntimeException> exception =
                detection.checkConflicts(
                        snapshot(1),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonList(
                                createGlobalIndexEntry("idx", ADD, BinaryRow.EMPTY_ROW, 0, 149)),
                        null,
                        Snapshot.CommitKind.APPEND);

        assertThat(exception).isEmpty();
    }

    @Test
    public void testBuildBaseEntriesWithDV() {
        {
            // Scene 1
            List<SimpleFileEntry> baseEntries = new ArrayList<>();
            baseEntries.add(createFileEntry("f1", ADD));
            baseEntries.add(createFileEntry("f2", ADD));

            List<IndexManifestEntry> deltaIndexEntries = new ArrayList<>();
            deltaIndexEntries.add(createDvIndexEntry("dv1", ADD, Arrays.asList("f2")));

            assertThat(buildBaseEntriesWithDV(baseEntries, deltaIndexEntries))
                    .containsExactlyInAnyOrder(
                            createFileEntryWithDV("f1", ADD, null),
                            createFileEntryWithDV("f2", ADD, "dv1"));
        }

        {
            // Scene 2: skip delete dv
            List<SimpleFileEntry> baseEntries = new ArrayList<>();
            baseEntries.add(createFileEntry("f1", ADD));
            baseEntries.add(createFileEntry("f2", ADD));

            List<IndexManifestEntry> deltaIndexEntries = new ArrayList<>();
            deltaIndexEntries.add(createDvIndexEntry("dv1", DELETE, Arrays.asList("f2")));

            assertThat(buildBaseEntriesWithDV(baseEntries, deltaIndexEntries))
                    .containsExactlyInAnyOrder(
                            createFileEntryWithDV("f1", ADD, null),
                            createFileEntryWithDV("f2", ADD, null));
        }
    }

    @Test
    public void testBuildDeltaEntriesWithDV() {
        {
            // Scene 1: update f2's dv
            List<SimpleFileEntry> baseEntries = new ArrayList<>();
            baseEntries.add(createFileEntryWithDV("f1", ADD, "dv1"));
            baseEntries.add(createFileEntryWithDV("f2", ADD, null));

            List<SimpleFileEntry> deltaEntries = new ArrayList<>();
            deltaEntries.add(createFileEntry("f2", DELETE));
            deltaEntries.add(createFileEntry("f2_new", ADD));

            List<IndexManifestEntry> deltaIndexEntries = new ArrayList<>();
            deltaIndexEntries.add(createDvIndexEntry("dv2", ADD, Arrays.asList("f2_new")));

            assertThat(buildDeltaEntriesWithDV(baseEntries, deltaEntries, deltaIndexEntries))
                    .containsExactlyInAnyOrder(
                            createFileEntryWithDV("f2", DELETE, null),
                            createFileEntryWithDV("f2_new", ADD, "dv2"));
        }

        {
            // Scene 2: update f2 and merge f1's dv
            List<SimpleFileEntry> baseEntries = new ArrayList<>();
            baseEntries.add(createFileEntryWithDV("f1", ADD, "dv1"));
            baseEntries.add(createFileEntryWithDV("f2", ADD, null));

            List<SimpleFileEntry> deltaEntries = new ArrayList<>();
            deltaEntries.add(createFileEntry("f2", DELETE));
            deltaEntries.add(createFileEntry("f2_new", ADD));
            deltaEntries.add(createFileEntry("f3", ADD));

            List<IndexManifestEntry> deltaIndexEntries = new ArrayList<>();
            deltaIndexEntries.add(createDvIndexEntry("dv1", DELETE, Arrays.asList("f1")));
            deltaIndexEntries.add(createDvIndexEntry("dv2", ADD, Arrays.asList("f1", "f2_new")));

            assertThat(buildDeltaEntriesWithDV(baseEntries, deltaEntries, deltaIndexEntries))
                    .containsExactlyInAnyOrder(
                            createFileEntryWithDV("f1", DELETE, "dv1"),
                            createFileEntryWithDV("f1", ADD, "dv2"),
                            createFileEntryWithDV("f2", DELETE, null),
                            createFileEntryWithDV("f2_new", ADD, "dv2"),
                            createFileEntryWithDV("f3", ADD, null));
        }

        {
            // Scene 3: update f2 (with dv) and merge f1's dv
            List<SimpleFileEntry> baseEntries = new ArrayList<>();
            baseEntries.add(createFileEntryWithDV("f1", ADD, "dv1"));
            baseEntries.add(createFileEntryWithDV("f2", ADD, "dv2"));

            List<SimpleFileEntry> deltaEntries = new ArrayList<>();
            deltaEntries.add(createFileEntry("f2", DELETE));
            deltaEntries.add(createFileEntry("f2_new", ADD));
            deltaEntries.add(createFileEntry("f3", ADD));

            List<IndexManifestEntry> deltaIndexEntries = new ArrayList<>();
            deltaIndexEntries.add(createDvIndexEntry("dv1", DELETE, Arrays.asList("f1")));
            deltaIndexEntries.add(createDvIndexEntry("dv2", DELETE, Arrays.asList("f2")));
            deltaIndexEntries.add(createDvIndexEntry("dv3", ADD, Arrays.asList("f1", "f2_new")));

            assertThat(buildDeltaEntriesWithDV(baseEntries, deltaEntries, deltaIndexEntries))
                    .containsExactlyInAnyOrder(
                            createFileEntryWithDV("f1", DELETE, "dv1"),
                            createFileEntryWithDV("f1", ADD, "dv3"),
                            createFileEntryWithDV("f2", DELETE, "dv2"),
                            createFileEntryWithDV("f2_new", ADD, "dv3"),
                            createFileEntryWithDV("f3", ADD, null));
        }

        {
            // Scene 4: full compact
            List<SimpleFileEntry> baseEntries = new ArrayList<>();
            baseEntries.add(createFileEntryWithDV("f1", ADD, null));
            baseEntries.add(createFileEntryWithDV("f2", ADD, "dv1"));
            baseEntries.add(createFileEntryWithDV("f3", ADD, "dv1"));
            baseEntries.add(createFileEntryWithDV("f4", ADD, "dv2"));

            List<SimpleFileEntry> deltaEntries = new ArrayList<>();
            deltaEntries.add(createFileEntry("f1", DELETE));
            deltaEntries.add(createFileEntry("f2", DELETE));
            deltaEntries.add(createFileEntry("f3", DELETE));
            deltaEntries.add(createFileEntry("f4", DELETE));
            deltaEntries.add(createFileEntry("f5_compact", ADD));

            List<IndexManifestEntry> deltaIndexEntries = new ArrayList<>();
            deltaIndexEntries.add(createDvIndexEntry("dv1", DELETE, Arrays.asList("f2", "f3")));
            deltaIndexEntries.add(createDvIndexEntry("dv2", DELETE, Arrays.asList("f4")));

            assertThat(buildDeltaEntriesWithDV(baseEntries, deltaEntries, deltaIndexEntries))
                    .containsExactlyInAnyOrder(
                            createFileEntryWithDV("f1", DELETE, null),
                            createFileEntryWithDV("f2", DELETE, "dv1"),
                            createFileEntryWithDV("f3", DELETE, "dv1"),
                            createFileEntryWithDV("f4", DELETE, "dv2"),
                            createFileEntryWithDV("f5_compact", ADD, null));
        }

        {
            // Scene 5: merge into with update, delete and insert
            List<SimpleFileEntry> baseEntries = new ArrayList<>();
            baseEntries.add(createFileEntryWithDV("f1", ADD, null));
            baseEntries.add(createFileEntryWithDV("f2", ADD, null));
            baseEntries.add(createFileEntryWithDV("f3", ADD, "dv1"));
            baseEntries.add(createFileEntryWithDV("f4", ADD, "dv1"));
            baseEntries.add(createFileEntryWithDV("f5", ADD, "dv2"));

            List<SimpleFileEntry> deltaEntries = new ArrayList<>();
            deltaEntries.add(createFileEntry("f2", DELETE));
            deltaEntries.add(createFileEntry("f3", DELETE));
            deltaEntries.add(createFileEntry("f3_new", ADD));
            deltaEntries.add(createFileEntry("f7", ADD));

            List<IndexManifestEntry> deltaIndexEntries = new ArrayList<>();
            deltaIndexEntries.add(createDvIndexEntry("dv1", DELETE, Arrays.asList("f3", "f4")));
            deltaIndexEntries.add(createDvIndexEntry("dv2", DELETE, Arrays.asList("f5")));
            deltaIndexEntries.add(createDvIndexEntry("dv3", ADD, Arrays.asList("f1", "f4", "f5")));

            assertThat(buildDeltaEntriesWithDV(baseEntries, deltaEntries, deltaIndexEntries))
                    .containsExactlyInAnyOrder(
                            createFileEntryWithDV("f1", DELETE, null),
                            createFileEntryWithDV("f1", ADD, "dv3"),
                            createFileEntryWithDV("f2", DELETE, null),
                            createFileEntryWithDV("f3", DELETE, "dv1"),
                            createFileEntryWithDV("f3_new", ADD, null),
                            createFileEntryWithDV("f4", DELETE, "dv1"),
                            createFileEntryWithDV("f4", ADD, "dv3"),
                            createFileEntryWithDV("f5", DELETE, "dv2"),
                            createFileEntryWithDV("f5", ADD, "dv3"),
                            createFileEntryWithDV("f7", ADD, null));
        }
    }

    @Test
    public void testConflictDeletionWithDV() {
        {
            // Scene 1: base -------------> update2 (conflict)
            //           f1          ^         <f1, +dv2>
            //                       |
            //                  update1 (finished)
            //                    <f1, +dv1>
            List<SimpleFileEntry> update1Entries = new ArrayList<>();
            update1Entries.add(createFileEntryWithDV("f1", ADD, "dv1"));

            List<SimpleFileEntry> update2DeltaEntries = new ArrayList<>();

            List<IndexManifestEntry> update2DeltaIndexEntries = new ArrayList<>();
            update2DeltaIndexEntries.add(createDvIndexEntry("dv2", ADD, Arrays.asList("f1")));

            List<SimpleFileEntry> update2DeltaEntriesWithDV =
                    buildDeltaEntriesWithDV(
                            update1Entries, update2DeltaEntries, update2DeltaIndexEntries);
            assertThat(update2DeltaEntriesWithDV)
                    .containsExactlyInAnyOrder(
                            createFileEntryWithDV("f1", DELETE, null),
                            createFileEntryWithDV("f1", ADD, "dv2"));
            assertConflict(update1Entries, update2DeltaEntriesWithDV);
        }

        {
            // Scene 2: base -------------> update2 (conflict)
            //         <f1, dv0>     ^        <f1, +dv2>
            //                       |
            //                  update1 (finished)
            //                    <f1, +dv1>
            List<SimpleFileEntry> update1Entries = new ArrayList<>();
            update1Entries.add(createFileEntryWithDV("f1", ADD, "dv1"));

            List<SimpleFileEntry> update2DeltaEntries = new ArrayList<>();

            List<IndexManifestEntry> update2DeltaIndexEntries = new ArrayList<>();
            update2DeltaIndexEntries.add(createDvIndexEntry("dv0", DELETE, Arrays.asList("f1")));
            update2DeltaIndexEntries.add(createDvIndexEntry("dv2", ADD, Arrays.asList("f1")));

            List<SimpleFileEntry> update2DeltaEntriesWithDV =
                    buildDeltaEntriesWithDV(
                            update1Entries, update2DeltaEntries, update2DeltaIndexEntries);
            assertThat(update2DeltaEntriesWithDV)
                    .containsExactlyInAnyOrder(
                            createFileEntryWithDV("f1", DELETE, "dv0"),
                            createFileEntryWithDV("f1", ADD, "dv2"));
            assertConflict(update1Entries, update2DeltaEntriesWithDV);
        }

        {
            // Scene 3: base -------------> update2 (conflict)
            //         <f1, dv0>      ^     <-f1, -dv0>, <+f3, null>
            //                        |
            //                  update1 (finished)
            //                 <-f1, -dv0>, <+f2, dv1>
            List<SimpleFileEntry> update1Entries = new ArrayList<>();
            update1Entries.add(createFileEntryWithDV("f2", ADD, "dv1"));

            List<SimpleFileEntry> update2DeltaEntries = new ArrayList<>();
            update2DeltaEntries.add(createFileEntry("f1", DELETE));
            update2DeltaEntries.add(createFileEntry("f3", ADD));

            List<IndexManifestEntry> update2DeltaIndexEntries = new ArrayList<>();
            update2DeltaIndexEntries.add(createDvIndexEntry("dv0", DELETE, Arrays.asList("f1")));

            List<SimpleFileEntry> update2DeltaEntriesWithDV =
                    buildDeltaEntriesWithDV(
                            update1Entries, update2DeltaEntries, update2DeltaIndexEntries);
            assertThat(update2DeltaEntriesWithDV)
                    .containsExactlyInAnyOrder(
                            createFileEntryWithDV("f1", DELETE, "dv0"),
                            createFileEntryWithDV("f3", ADD, null));
            assertConflict(update1Entries, update2DeltaEntriesWithDV);
        }
    }

    private SimpleFileEntry createFileEntry(String fileName, FileKind kind) {
        return new SimpleFileEntry(
                kind,
                EMPTY_ROW,
                0,
                1,
                0,
                fileName,
                Collections.emptyList(),
                null,
                EMPTY_ROW,
                EMPTY_ROW,
                null,
                0L,
                null);
    }

    private SimpleFileEntry createLevelFileEntry(String fileName, int level) {
        return new SimpleFileEntry(
                ADD,
                EMPTY_ROW,
                0,
                1,
                level,
                fileName,
                Collections.emptyList(),
                null,
                EMPTY_ROW,
                EMPTY_ROW,
                null,
                0L,
                null);
    }

    private SimpleFileEntryWithDV createFileEntryWithDV(
            String fileName, FileKind kind, @Nullable String dvFileName) {
        return new SimpleFileEntryWithDV(createFileEntry(fileName, kind), dvFileName);
    }

    private IndexManifestEntry createDvIndexEntry(
            String fileName, FileKind kind, List<String> fileNames) {
        LinkedHashMap<String, DeletionVectorMeta> dvRanges = new LinkedHashMap<>();
        for (String name : fileNames) {
            dvRanges.put(name, new DeletionVectorMeta(name, 1, 1, 1L));
        }
        return new IndexManifestEntry(
                kind,
                EMPTY_ROW,
                0,
                new IndexFileMeta(
                        DELETION_VECTORS_INDEX, fileName, 11, dvRanges.size(), dvRanges, null));
    }

    private IndexManifestEntry createGlobalIndexEntry(
            String fileName, FileKind kind, BinaryRow partition, long from, long to) {
        return createGlobalIndexEntry(fileName, kind, partition, 0, from, to);
    }

    private IndexManifestEntry createGlobalIndexEntry(
            String fileName, FileKind kind, BinaryRow partition, int bucket, long from, long to) {
        return new IndexManifestEntry(
                kind,
                partition,
                bucket,
                new IndexFileMeta(
                        "btree",
                        fileName,
                        11,
                        1,
                        new GlobalIndexMeta(from, to, 0, null, null),
                        null));
    }

    private void assertConflict(
            List<SimpleFileEntry> baseEntries, List<SimpleFileEntry> deltaEntries) {
        ArrayList<SimpleFileEntry> simpleFileEntryWithDVS = new ArrayList<>(baseEntries);
        simpleFileEntryWithDVS.addAll(deltaEntries);
        Collection<SimpleFileEntry> merged = FileEntry.mergeEntries(simpleFileEntryWithDVS);
        int deleteCount = 0;
        for (SimpleFileEntry simpleFileEntryWithDV : merged) {
            if (simpleFileEntryWithDV.kind().equals(FileKind.DELETE)) {
                deleteCount++;
            }
        }
        assert (deleteCount > 0);
    }

    @Test
    void testShouldBeOverwriteCommit() {
        DataEvolutionConflictDetection detection = createConflictDetection();

        List<SimpleFileEntry> addOnlyEntries = new ArrayList<>();
        addOnlyEntries.add(createFileEntry("f1", ADD));
        addOnlyEntries.add(createFileEntry("f2", ADD));
        assertThat(detection.shouldBeOverwriteCommit(addOnlyEntries, Collections.emptyList()))
                .isFalse();

        assertThat(
                        detection.shouldBeOverwriteCommit(
                                Collections.emptyList(), Collections.emptyList()))
                .isFalse();

        List<SimpleFileEntry> deleteEntries = new ArrayList<>();
        deleteEntries.add(createFileEntry("f1", DELETE));
        deleteEntries.add(createFileEntry("f2", ADD));
        assertThat(detection.shouldBeOverwriteCommit(deleteEntries, Collections.emptyList()))
                .isTrue();

        List<IndexManifestEntry> dvIndexFiles = new ArrayList<>();
        dvIndexFiles.add(createDvIndexEntry("dv1", ADD, Arrays.asList("f1")));
        assertThat(detection.shouldBeOverwriteCommit(Collections.emptyList(), dvIndexFiles))
                .isTrue();

        detection.setRowIdCheckFromSnapshot(1L);
        assertThat(detection.shouldBeOverwriteCommit(addOnlyEntries, Collections.emptyList()))
                .isFalse();
    }

    @Test
    void testMaterializeDvRowIdCheckOnlyAppliesToCompactCommit() {
        DataEvolutionConflictDetection detection = createConflictDetection();

        detection.setRowIdCheckFromSnapshotForMaterializeDvCompaction(1L);
        assertThat(detection.shouldCheckRowIdFromSnapshot(Snapshot.CommitKind.APPEND)).isFalse();
        assertThat(detection.shouldCheckRowIdFromSnapshot(Snapshot.CommitKind.OVERWRITE)).isFalse();
        assertThat(detection.shouldCheckRowIdFromSnapshot(Snapshot.CommitKind.COMPACT)).isTrue();
        assertThat(detection.shouldCheckHistoricalRowIdEntry(FileKind.ADD)).isTrue();
        assertThat(detection.shouldCheckHistoricalRowIdEntry(FileKind.DELETE)).isFalse();

        RowIdConflictChecker checker =
                detection.createRowIdConflictChecker(
                        mock(SchemaManager.class),
                        Arrays.asList(
                                manifestEntry(DELETE, "deleted", 10L, new Range(10, 19)),
                                manifestEntry(ADD, "added", 30L, new Range(30, 39)),
                                manifestEntry(DELETE, "dedicated.blob", 50L, new Range(50, 59)),
                                manifestEntry(
                                        DELETE, "dedicated.vector.data", 70L, new Range(70, 79))),
                        Snapshot.CommitKind.COMPACT);
        assertThat(checker).isNotNull();
        assertThat(
                        checker.conflictsWith(
                                manifestEntry(ADD, "historical", 15L, new Range(15, 24)).file()))
                .isTrue();
        assertThat(
                        checker.conflictsWith(
                                manifestEntry(ADD, "historical", 35L, new Range(35, 44)).file()))
                .isFalse();
        assertThat(
                        checker.conflictsWith(
                                manifestEntry(ADD, "historical", 55L, new Range(55, 64)).file()))
                .isFalse();
        assertThat(
                        checker.conflictsWith(
                                manifestEntry(ADD, "historical", 75L, new Range(75, 84)).file()))
                .isFalse();

        detection.setRowIdCheckFromSnapshot(1L);
        assertThat(detection.shouldCheckRowIdFromSnapshot(Snapshot.CommitKind.APPEND)).isTrue();
        assertThat(detection.shouldCheckRowIdFromSnapshot(Snapshot.CommitKind.COMPACT)).isTrue();
        assertThat(detection.shouldCheckHistoricalRowIdEntry(FileKind.ADD)).isTrue();
        assertThat(detection.shouldCheckHistoricalRowIdEntry(FileKind.DELETE)).isTrue();
        assertThat(
                        detection.createRowIdConflictChecker(
                                mock(SchemaManager.class),
                                Collections.singletonList(manifestEntry(ADD, "added", null, null)),
                                Snapshot.CommitKind.APPEND))
                .isInstanceOf(RowIdColumnConflictChecker.class);

        detection.setRowIdCheckFromSnapshot(null);
        assertThat(detection.shouldCheckRowIdFromSnapshot(Snapshot.CommitKind.APPEND)).isFalse();
        assertThat(detection.shouldCheckRowIdFromSnapshot(Snapshot.CommitKind.COMPACT)).isFalse();
    }

    @Test
    void testMaterializeRowIdCheckSkipsCompactSnapshotsAndHistoricalDeletes() {
        CommitScanner scanner = mock(CommitScanner.class);
        SnapshotManager snapshotManager = mock(SnapshotManager.class);
        DataEvolutionConflictDetection detection =
                (DataEvolutionConflictDetection)
                        createConflictDetection(scanner, true, false, false, snapshotManager);
        detection.setRowIdCheckFromSnapshotForMaterializeDvCompaction(1L);

        Snapshot checkSnapshot = mock(Snapshot.class);
        Snapshot compactSnapshot = mock(Snapshot.class);
        Snapshot appendSnapshot = mock(Snapshot.class);
        Snapshot latestSnapshot = mock(Snapshot.class);
        when(checkSnapshot.nextRowId()).thenReturn(20L);
        when(compactSnapshot.commitKind()).thenReturn(Snapshot.CommitKind.COMPACT);
        when(appendSnapshot.commitKind()).thenReturn(Snapshot.CommitKind.APPEND);
        when(latestSnapshot.id()).thenReturn(3L);
        when(latestSnapshot.commitUser()).thenReturn("other-user");
        when(snapshotManager.snapshot(1)).thenReturn(checkSnapshot);
        when(snapshotManager.snapshot(2)).thenReturn(compactSnapshot);
        when(snapshotManager.snapshot(3)).thenReturn(appendSnapshot);

        ManifestEntry historicalDelete =
                manifestEntry(DELETE, "historical", 10L, new Range(10, 19));
        ManifestEntry historicalAdd = manifestEntry(ADD, "non-conflicting", 10L, new Range(10, 19));
        when(scanner.readIncrementalEntries(appendSnapshot, Collections.emptyList()))
                .thenReturn(Arrays.asList(historicalDelete, historicalAdd));
        RowIdConflictChecker checker = mock(RowIdConflictChecker.class);
        when(checker.isEmpty()).thenReturn(false);
        when(checker.conflictsWith(historicalDelete.file())).thenReturn(true);
        when(checker.conflictsWith(historicalAdd.file())).thenReturn(false);

        assertThat(
                        detection.checkConflicts(
                                latestSnapshot,
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                checker,
                                Snapshot.CommitKind.COMPACT))
                .isEmpty();
        verify(scanner, never()).readIncrementalEntries(compactSnapshot, Collections.emptyList());
        verify(scanner).readIncrementalEntries(appendSnapshot, Collections.emptyList());
        verify(checker, never()).conflictsWith(historicalDelete.file());
        verify(checker).conflictsWith(historicalAdd.file());
    }

    @Test
    void testChangedPartitionsIncludesGlobalIndexFiles() {
        BinaryRow partition = BinaryRow.singleColumn(1);

        assertThat(
                        ManifestEntryChanges.changedPartitions(
                                Collections.emptyList(),
                                Collections.singletonList(
                                        createGlobalIndexEntry("idx", ADD, partition, 0, 99))))
                .containsExactly(partition);
    }

    @Test
    void testCheckRowIdExistenceNoConflict() {
        DataEvolutionConflictDetection detection = createConflictDetection();

        List<SimpleFileEntry> baseEntries = new ArrayList<>();
        baseEntries.add(createFileEntryWithRowId("f1", ADD, 0L, 100L));

        List<SimpleFileEntry> deltaEntries = new ArrayList<>();
        deltaEntries.add(createFileEntryWithRowId("p1", ADD, 0L, 100L));

        assertThat(
                        detection.checkRowIdExistence(
                                baseEntries, deltaEntries, 100L, Snapshot.CommitKind.APPEND))
                .isEmpty();
    }

    @Test
    void testCheckRowIdExistenceBaseFileRemoved() {
        DataEvolutionConflictDetection detection = createConflictDetection();

        List<SimpleFileEntry> baseEntries = new ArrayList<>();

        List<SimpleFileEntry> deltaEntries = new ArrayList<>();
        deltaEntries.add(createFileEntryWithRowId("p1", ADD, 0L, 100L));

        Optional<RuntimeException> result =
                detection.checkRowIdExistence(
                        baseEntries, deltaEntries, 100L, Snapshot.CommitKind.APPEND);
        assertThat(result).isPresent();
        assertThat(result.get())
                .isInstanceOf(RowIdExistenceConflictException.class)
                .hasMessageContaining("Row ID existence conflict");
    }

    @Test
    void testCheckRowIdExistenceBaseFileRewritten() {
        DataEvolutionConflictDetection detection = createConflictDetection();

        List<SimpleFileEntry> baseEntries = new ArrayList<>();
        baseEntries.add(createFileEntryWithRowId("f2", ADD, 0L, 200L));

        List<SimpleFileEntry> deltaEntries = new ArrayList<>();
        deltaEntries.add(createFileEntryWithRowId("p1", ADD, 0L, 100L));

        Optional<RuntimeException> result =
                detection.checkRowIdExistence(
                        baseEntries, deltaEntries, 200L, Snapshot.CommitKind.APPEND);
        assertThat(result).isPresent();
        assertThat(result.get())
                .isInstanceOf(RowIdExistenceConflictException.class)
                .hasMessageContaining("Row ID existence conflict");
    }

    @Test
    void testCheckRowIdExistenceNormalFileRejectsAdjacentDataFiles() {
        DataEvolutionConflictDetection detection = createConflictDetection();

        List<SimpleFileEntry> baseEntries = new ArrayList<>();
        baseEntries.add(createFileEntryWithRowId("f1", ADD, 0L, 2L));
        baseEntries.add(createFileEntryWithRowId("f2", ADD, 2L, 2L));

        List<SimpleFileEntry> deltaEntries = new ArrayList<>();
        deltaEntries.add(createFileEntryWithRowId("p1", ADD, 0L, 4L));

        Optional<RuntimeException> result =
                detection.checkRowIdExistence(
                        baseEntries, deltaEntries, 4L, Snapshot.CommitKind.APPEND);
        assertThat(result).isPresent();
        assertThat(result.get().getMessage()).contains("Row ID existence conflict");
    }

    @Test
    void testCheckRowIdExistenceDedicatedFileCoveredByDataFiles() {
        DataEvolutionConflictDetection detection = createConflictDetection();

        List<SimpleFileEntry> baseEntries = new ArrayList<>();
        baseEntries.add(createFileEntryWithRowId("f1", ADD, 0L, 4L));

        List<SimpleFileEntry> deltaEntries = new ArrayList<>();
        deltaEntries.add(createFileEntryWithRowId("p1.blob", ADD, 0L, 2L));

        assertThat(
                        detection.checkRowIdExistence(
                                baseEntries, deltaEntries, 4L, Snapshot.CommitKind.APPEND))
                .isEmpty();
    }

    @Test
    void testCheckRowIdExistenceDedicatedFileRejectsAdjacentDataFiles() {
        DataEvolutionConflictDetection detection = createConflictDetection();

        List<SimpleFileEntry> baseEntries = new ArrayList<>();
        baseEntries.add(createFileEntryWithRowId("f1", ADD, 0L, 2L));
        baseEntries.add(createFileEntryWithRowId("f2", ADD, 2L, 2L));

        List<SimpleFileEntry> deltaEntries = new ArrayList<>();
        deltaEntries.add(createFileEntryWithRowId("p1.blob", ADD, 0L, 4L));

        Optional<RuntimeException> result =
                detection.checkRowIdExistence(
                        baseEntries, deltaEntries, 4L, Snapshot.CommitKind.APPEND);
        assertThat(result).isPresent();
        assertThat(result.get().getMessage()).contains("Row ID existence conflict");
    }

    @Test
    void testCheckRowIdExistenceDedicatedFileRejectsRangeNotCoveredByOneDataFile() {
        DataEvolutionConflictDetection detection = createConflictDetection();

        List<SimpleFileEntry> baseEntries = new ArrayList<>();
        baseEntries.add(createFileEntryWithRowId("f1", ADD, 0L, 2L));

        List<SimpleFileEntry> deltaEntries = new ArrayList<>();
        deltaEntries.add(createFileEntryWithRowId("p1.blob", ADD, 0L, 3L));

        Optional<RuntimeException> result =
                detection.checkRowIdExistence(
                        baseEntries, deltaEntries, 3L, Snapshot.CommitKind.APPEND);
        assertThat(result).isPresent();
        assertThat(result.get().getMessage()).contains("Row ID existence conflict");
    }

    @Test
    void testCheckRowIdExistenceDedicatedFileIgnoresBaseDedicatedFiles() {
        DataEvolutionConflictDetection detection = createConflictDetection();

        List<SimpleFileEntry> baseEntries = new ArrayList<>();
        baseEntries.add(createFileEntryWithRowId("old.blob", ADD, 0L, 2L));

        List<SimpleFileEntry> deltaEntries = new ArrayList<>();
        deltaEntries.add(createFileEntryWithRowId("p1.blob", ADD, 0L, 2L));

        Optional<RuntimeException> result =
                detection.checkRowIdExistence(
                        baseEntries, deltaEntries, 2L, Snapshot.CommitKind.APPEND);
        assertThat(result).isPresent();
        assertThat(result.get().getMessage()).contains("Row ID existence conflict");
    }

    @Test
    void testCheckRowIdExistenceSkipsNewlyAppendedFiles() {
        DataEvolutionConflictDetection detection = createConflictDetection();

        // nextRowId=100: files with firstRowId >= 100 are newly appended, not references
        List<SimpleFileEntry> baseEntries = new ArrayList<>();
        baseEntries.add(createFileEntryWithRowId("f1", ADD, 0L, 100L));

        List<SimpleFileEntry> deltaEntries = new ArrayList<>();
        // partial-column update referencing existing rows (firstRowId=0 < nextRowId=100)
        deltaEntries.add(createFileEntryWithRowId("p1", ADD, 0L, 100L));
        // newly appended file (firstRowId=100 >= nextRowId=100), should be skipped
        deltaEntries.add(createFileEntryWithRowId("new1", ADD, 100L, 50L));

        assertThat(
                        detection.checkRowIdExistence(
                                baseEntries, deltaEntries, 100L, Snapshot.CommitKind.APPEND))
                .isEmpty();
    }

    @Test
    void testCheckRowIdExistenceSkipsNonPreAssigned() {
        DataEvolutionConflictDetection detection = createConflictDetection();

        List<SimpleFileEntry> baseEntries = new ArrayList<>();

        List<SimpleFileEntry> deltaEntries = new ArrayList<>();
        deltaEntries.add(createFileEntry("f1", ADD));

        assertThat(
                        detection.checkRowIdExistence(
                                baseEntries, deltaEntries, 100L, Snapshot.CommitKind.APPEND))
                .isEmpty();
    }

    @Test
    void testCheckRowIdExistenceSkipsDeleteEntries() {
        DataEvolutionConflictDetection detection = createConflictDetection();

        List<SimpleFileEntry> baseEntries = new ArrayList<>();

        List<SimpleFileEntry> deltaEntries = new ArrayList<>();
        deltaEntries.add(createFileEntryWithRowId("f1", DELETE, 0L, 100L));

        assertThat(
                        detection.checkRowIdExistence(
                                baseEntries, deltaEntries, 100L, Snapshot.CommitKind.APPEND))
                .isEmpty();
    }

    @Test
    void testCheckRowIdExistenceRejectsStaleDeleteAfterReassign() {
        DataEvolutionConflictDetection detection = createConflictDetection();

        List<SimpleFileEntry> baseEntries =
                Collections.singletonList(createFileEntryWithRowId("f1", ADD, 100L, 10L));
        List<SimpleFileEntry> deltaEntries =
                Collections.singletonList(createFileEntryWithRowId("f1", DELETE, 0L, 10L));

        Optional<RuntimeException> result =
                detection.checkConflicts(
                        snapshot(1),
                        baseEntries,
                        deltaEntries,
                        Collections.emptyList(),
                        null,
                        Snapshot.CommitKind.APPEND);

        assertThat(result).isPresent();
        assertThat(result.get())
                .isInstanceOf(RowIdExistenceConflictException.class)
                .hasMessageContaining("DELETE for file 'f1'")
                .hasMessageContaining("firstRowId=0, rowCount=10")
                .hasMessageContaining("firstRowId=100, rowCount=10");
    }

    @Test
    void testCheckRowIdExistenceRejectsStaleDeleteWithDifferentRowCount() {
        DataEvolutionConflictDetection detection = createConflictDetection();

        List<SimpleFileEntry> baseEntries =
                Collections.singletonList(createFileEntryWithRowId("f1", ADD, 0L, 20L));
        List<SimpleFileEntry> deltaEntries =
                Collections.singletonList(createFileEntryWithRowId("f1", DELETE, 0L, 10L));

        Optional<RuntimeException> result =
                detection.checkConflicts(
                        snapshot(1),
                        baseEntries,
                        deltaEntries,
                        Collections.emptyList(),
                        null,
                        Snapshot.CommitKind.APPEND);

        assertThat(result).isPresent();
        assertThat(result.get()).isInstanceOf(RowIdExistenceConflictException.class);
    }

    @Test
    void testCheckRowIdExistenceAllowsDeleteWithCurrentAssignment() {
        DataEvolutionConflictDetection detection = createConflictDetection();

        List<SimpleFileEntry> baseEntries =
                Collections.singletonList(createFileEntryWithRowId("f1", ADD, 100L, 10L));
        List<SimpleFileEntry> deltaEntries =
                Collections.singletonList(createFileEntryWithRowId("f1", DELETE, 100L, 10L));

        assertThat(
                        detection.checkConflicts(
                                snapshot(1),
                                baseEntries,
                                deltaEntries,
                                Collections.emptyList(),
                                null,
                                Snapshot.CommitKind.APPEND))
                .isEmpty();
    }

    @Test
    void testCheckRowIdExistenceSkipsWhenNextRowIdNull() {
        DataEvolutionConflictDetection detection = createConflictDetection();

        List<SimpleFileEntry> baseEntries = new ArrayList<>();
        List<SimpleFileEntry> deltaEntries = new ArrayList<>();
        deltaEntries.add(createFileEntryWithRowId("p1", ADD, 0L, 100L));

        assertThat(
                        detection.checkRowIdExistence(
                                baseEntries, deltaEntries, null, Snapshot.CommitKind.APPEND))
                .isEmpty();
    }

    @Test
    void testCheckRowIdExistenceCompactAllowsAdjacentNormalRanges() {
        DataEvolutionConflictDetection detection = createConflictDetection();

        List<SimpleFileEntry> baseEntries =
                Arrays.asList(
                        createFileEntryWithRowId("f1", ADD, 0L, 2L),
                        createFileEntryWithRowId("f2", ADD, 2L, 2L));
        List<SimpleFileEntry> deltaEntries =
                Collections.singletonList(createFileEntryWithRowId("compacted", ADD, 0L, 4L));

        assertThat(
                        detection.checkRowIdExistence(
                                baseEntries, deltaEntries, 4L, Snapshot.CommitKind.COMPACT))
                .isEmpty();
    }

    @Test
    void testCheckRowIdExistenceCompactAllowsBlobAcrossAdjacentNormalRanges() {
        DataEvolutionConflictDetection detection = createConflictDetection();

        List<SimpleFileEntry> baseEntries =
                Arrays.asList(
                        createFileEntryWithRowId("f1", ADD, 0L, 2L),
                        createFileEntryWithRowId("f2", ADD, 2L, 2L));
        List<SimpleFileEntry> deltaEntries =
                Collections.singletonList(createFileEntryWithRowId("compacted.blob", ADD, 0L, 4L));

        assertThat(
                        detection.checkRowIdExistence(
                                baseEntries, deltaEntries, 4L, Snapshot.CommitKind.COMPACT))
                .isEmpty();
    }

    @Test
    void testCheckRowIdExistenceCompactRejectsStaleRangeAfterReassign() {
        DataEvolutionConflictDetection detection = createConflictDetection();

        List<SimpleFileEntry> baseEntries =
                Arrays.asList(
                        createFileEntryWithRowId("f1", ADD, 10L, 2L),
                        createFileEntryWithRowId("f2", ADD, 12L, 2L));
        List<SimpleFileEntry> deltaEntries =
                Collections.singletonList(createFileEntryWithRowId("compacted", ADD, 0L, 4L));

        Optional<RuntimeException> result =
                detection.checkConflicts(
                        snapshot(1),
                        baseEntries,
                        deltaEntries,
                        Collections.emptyList(),
                        null,
                        Snapshot.CommitKind.COMPACT);
        assertThat(result).isPresent();
        assertThat(result.get()).hasMessageContaining("Row ID existence conflict");
    }

    @Test
    void testCheckRowIdExistenceCompactDoesNotMergeAcrossPartitions() {
        DataEvolutionConflictDetection detection = createConflictDetection();
        BinaryRow partition0 = BinaryRow.singleColumn(0);
        BinaryRow partition1 = BinaryRow.singleColumn(1);

        List<SimpleFileEntry> baseEntries =
                Arrays.asList(
                        createFileEntryWithRowId("f1", ADD, partition0, 0, 0L, 2L),
                        createFileEntryWithRowId("f2", ADD, partition1, 0, 2L, 2L));
        List<SimpleFileEntry> deltaEntries =
                Collections.singletonList(
                        createFileEntryWithRowId("compacted", ADD, partition0, 0, 0L, 4L));

        Optional<RuntimeException> result =
                detection.checkRowIdExistence(
                        baseEntries, deltaEntries, 4L, Snapshot.CommitKind.COMPACT);
        assertThat(result).isPresent();
        assertThat(result.get()).hasMessageContaining("Row ID existence conflict");
    }

    @Test
    void testCheckRowIdRangeConflictsUsesRetryableExceptionForDataFiles() {
        DataEvolutionConflictDetection detection = createConflictDetection();

        Optional<RuntimeException> exception =
                detection.checkConflicts(
                        snapshot(1),
                        Arrays.asList(
                                createFileEntryWithRowId("f1", ADD, 0L, 2L),
                                createFileEntryWithRowId("f2", ADD, 2L, 2L)),
                        Collections.singletonList(
                                createFileEntryWithRowId("compacted", ADD, 0L, 4L)),
                        Collections.emptyList(),
                        null,
                        Snapshot.CommitKind.COMPACT);

        assertThat(exception).isPresent();
        assertThat(exception.get()).isInstanceOf(DataEvolutionRowRangeConflictException.class);
    }

    @Test
    void testCheckRowIdRangeConflictsReportsDedicatedFileSpanningDataFiles() {
        DataEvolutionConflictDetection detection = createConflictDetection();

        Optional<RuntimeException> exception =
                detection.checkConflicts(
                        snapshot(1),
                        Arrays.asList(
                                createFileEntryWithRowId("f1", ADD, 0L, 2L),
                                createFileEntryWithRowId("f2", ADD, 2L, 2L)),
                        Collections.singletonList(createFileEntryWithRowId("p1.blob", ADD, 0L, 4L)),
                        Collections.emptyList(),
                        null,
                        Snapshot.CommitKind.COMPACT);

        assertThat(exception).isPresent();
        assertThat(exception.get())
                .isNotInstanceOf(DataEvolutionRowRangeConflictException.class)
                .hasMessageContaining("dedicated file")
                .hasMessageContaining("p1.blob")
                .hasMessageContaining("spans multiple data file ranges")
                .hasMessageContaining("f1")
                .hasMessageContaining("f2");
    }

    @Test
    void testCheckRowIdRangeConflictsRejectsOverlappingNormalFiles() {
        DataEvolutionConflictDetection detection = createConflictDetection();

        Optional<RuntimeException> exception =
                detection.checkConflicts(
                        snapshot(1),
                        Collections.singletonList(createFileEntryWithRowId("base", ADD, 0L, 5L)),
                        Collections.singletonList(
                                createFileEntryWithRowId("compacted", ADD, 2L, 2L)),
                        Collections.emptyList(),
                        null,
                        Snapshot.CommitKind.COMPACT);

        assertThat(exception).isPresent();
        assertThat(exception.get())
                .hasMessageContaining("multiple 'MERGE INTO' and 'COMPACT' operations")
                .hasMessageContaining("base")
                .hasMessageContaining("compacted");
    }

    @Test
    void testCheckRowIdRangeConflictsAllowsAdjacentDataFiles() {
        DataEvolutionConflictDetection detection = createConflictDetection();

        Optional<RuntimeException> exception =
                detection.checkConflicts(
                        snapshot(1),
                        Arrays.asList(
                                createFileEntryWithRowId("f1", ADD, 0L, 2L),
                                createFileEntryWithRowId("f2", ADD, 2L, 2L)),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        null,
                        Snapshot.CommitKind.COMPACT);

        assertThat(exception).isEmpty();
    }

    @Test
    void testCheckRowIdRangeConflictsAllowsDedicatedFileCoveredByOneDataFile() {
        DataEvolutionConflictDetection detection = createConflictDetection();

        Optional<RuntimeException> exception =
                detection.checkConflicts(
                        snapshot(1),
                        Collections.singletonList(createFileEntryWithRowId("f1", ADD, 0L, 4L)),
                        Collections.singletonList(createFileEntryWithRowId("p1.blob", ADD, 1L, 2L)),
                        Collections.emptyList(),
                        null,
                        Snapshot.CommitKind.COMPACT);

        assertThat(exception).isEmpty();
    }

    private SimpleFileEntry createFileEntryWithRowId(
            String fileName, FileKind kind, long firstRowId, long rowCount) {
        return createFileEntryWithRowId(fileName, kind, EMPTY_ROW, 0, firstRowId, rowCount);
    }

    private SimpleFileEntry createFileEntryWithRowId(
            String fileName,
            FileKind kind,
            BinaryRow partition,
            int bucket,
            long firstRowId,
            long rowCount) {
        return new SimpleFileEntry(
                kind,
                partition,
                bucket,
                1,
                0,
                fileName,
                Collections.emptyList(),
                null,
                EMPTY_ROW,
                EMPTY_ROW,
                null,
                rowCount,
                firstRowId);
    }

    @Test
    void testCheckGlobalIndexRowIdExistenceNoConflict() {
        DataEvolutionConflictDetection detection = createConflictDetection();

        Optional<RuntimeException> exception =
                detection.checkConflicts(
                        snapshot(1),
                        Arrays.asList(
                                createFileEntryWithRowId("f1", ADD, 0L, 100L),
                                createFileEntryWithRowId("f2", ADD, 100L, 50L)),
                        Collections.emptyList(),
                        Collections.singletonList(
                                createGlobalIndexEntry("idx", ADD, BinaryRow.EMPTY_ROW, 0, 149)),
                        null,
                        Snapshot.CommitKind.APPEND);

        assertThat(exception).isNotPresent();
    }

    @Test
    void testCheckGlobalIndexRowIdExistenceBaseFileRemoved() {
        DataEvolutionConflictDetection detection = createConflictDetection();

        Optional<RuntimeException> exception =
                detection.checkConflicts(
                        snapshot(1),
                        Collections.singletonList(createFileEntryWithRowId("f1", ADD, 0L, 100L)),
                        Collections.emptyList(),
                        Collections.singletonList(
                                createGlobalIndexEntry("idx", ADD, BinaryRow.EMPTY_ROW, 0, 149)),
                        null,
                        Snapshot.CommitKind.APPEND);

        assertThat(exception).isPresent();
        assertThat(exception.get())
                .hasMessageContaining("Global index row ID existence conflict")
                .hasMessageContaining("idx")
                .hasMessageContaining("[0, 149]");
    }

    @Test
    void testCheckGlobalIndexRowIdExistenceByPartitionAndBucket() {
        DataEvolutionConflictDetection detection = createConflictDetection();
        BinaryRow partition0 = BinaryRow.singleColumn(0);
        BinaryRow partition1 = BinaryRow.singleColumn(1);

        Optional<RuntimeException> exception =
                detection.checkConflicts(
                        snapshot(1),
                        Collections.singletonList(
                                createFileEntryWithRowId("f1", ADD, partition1, 0, 0L, 150L)),
                        Collections.emptyList(),
                        Collections.singletonList(
                                createGlobalIndexEntry("idx", ADD, partition0, 0, 0, 149)),
                        null,
                        Snapshot.CommitKind.APPEND);

        assertThat(exception).isPresent();

        exception =
                detection.checkConflicts(
                        snapshot(1),
                        Collections.singletonList(
                                createFileEntryWithRowId("f1", ADD, partition0, 1, 0L, 150L)),
                        Collections.emptyList(),
                        Collections.singletonList(
                                createGlobalIndexEntry("idx", ADD, partition0, 0, 0, 149)),
                        null,
                        Snapshot.CommitKind.APPEND);

        assertThat(exception).isPresent();
    }

    @Test
    void testCheckGlobalIndexRowIdExistenceSkipsDeleteIndexEntry() {
        DataEvolutionConflictDetection detection = createConflictDetection();

        Optional<RuntimeException> exception =
                detection.checkConflicts(
                        snapshot(1),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonList(
                                createGlobalIndexEntry("idx", DELETE, BinaryRow.EMPTY_ROW, 0, 149)),
                        null,
                        Snapshot.CommitKind.APPEND);

        assertThat(exception).isNotPresent();
    }

    private DataEvolutionConflictDetection createConflictDetection() {
        return (DataEvolutionConflictDetection) createConflictDetection(null, true, false);
    }

    private ConflictDetection createConflictDetection(
            @Nullable CommitScanner scanner,
            boolean dataEvolutionEnabled,
            boolean primaryKeyTable) {
        return createConflictDetection(scanner, dataEvolutionEnabled, primaryKeyTable, false, null);
    }

    private ConflictDetection createConflictDetection(
            @Nullable CommitScanner scanner,
            boolean dataEvolutionEnabled,
            boolean primaryKeyTable,
            boolean pkClusteringOverride,
            @Nullable SnapshotManager snapshotManager) {
        return ConflictDetection.create(
                "test-table",
                "test-user",
                RowType.of(),
                null,
                primaryKeyTable ? (left, right) -> 0 : null,
                BucketMode.HASH_FIXED,
                false,
                dataEvolutionEnabled,
                pkClusteringOverride,
                null,
                snapshotManager,
                scanner);
    }

    private CommitFailRetryResult commitFailRetryResult(
            @Nullable Snapshot latestSnapshot, @Nullable List<SimpleFileEntry> baseDataFiles) {
        return (CommitFailRetryResult)
                RetryCommitResult.forCommitFail(
                        latestSnapshot,
                        baseDataFiles,
                        new RuntimeException("expected test retry"),
                        null);
    }

    private ManifestEntry manifestEntry(
            FileKind kind, String fileName, @Nullable Long firstRowId, @Nullable Range rowIdRange) {
        ManifestEntry entry = mock(ManifestEntry.class);
        DataFileMeta file = mock(DataFileMeta.class);
        when(entry.kind()).thenReturn(kind);
        when(entry.file()).thenReturn(file);
        when(file.fileName()).thenReturn(fileName);
        when(file.firstRowId()).thenReturn(firstRowId);
        if (rowIdRange != null) {
            when(file.nonNullRowIdRange()).thenReturn(rowIdRange);
        }
        return entry;
    }

    private Snapshot snapshot(long id) {
        return new Snapshot(
                id,
                0,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                "commit-user",
                id,
                Snapshot.CommitKind.APPEND,
                id,
                0,
                0,
                null,
                null,
                null,
                null,
                null,
                null);
    }
}
