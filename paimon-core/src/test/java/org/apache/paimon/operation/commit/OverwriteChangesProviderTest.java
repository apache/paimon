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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.Snapshot;
import org.apache.paimon.TestFileStore;
import org.apache.paimon.TestKeyValueGenerator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.mergetree.compact.DeduplicateMergeFunction;
import org.apache.paimon.operation.FileStoreCommitImpl;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link OverwriteChangesProvider}. */
public class OverwriteChangesProviderTest {

    private TestKeyValueGenerator gen;
    @TempDir java.nio.file.Path tempDir;

    @BeforeEach
    public void beforeEach() {
        gen = new TestKeyValueGenerator();
    }

    @Test
    public void testManifestEntriesCache() throws Exception {
        TestFileStore store = createStore(1, Collections.emptyMap());
        KeyValue targetRecord = record("20260501", 8);
        BinaryRow targetPartition = gen.getPartition(targetRecord);
        store.commitData(
                Arrays.asList(targetRecord, record("20260502", 8)), gen::getPartition, kv -> 0);

        Snapshot snapshot1 = store.snapshotManager().latestSnapshot();
        PartitionPredicate partitionFilter = partitionFilter(store, targetPartition);
        OverwriteChangesProvider provider =
                provider(store, partitionFilter, Collections.emptyList());

        CommitChanges first = provider.provide(snapshot1);
        assertThat(provider.fullScanManifestCount).isEqualTo(1);
        assertThat(provider.deltaProbeCount).isZero();

        store.commitData(
                Collections.singletonList(record("20260502", 8)), gen::getPartition, kv -> 0);
        Snapshot snapshot2 = store.snapshotManager().latestSnapshot();
        CommitChanges afterUnrelatedAppend = provider.provide(snapshot2);

        assertThat(provider.fullScanManifestCount).isEqualTo(1);
        assertThat(provider.deltaProbeCount).isEqualTo(1);
        assertThat(identifiers(afterUnrelatedAppend.tableFiles))
                .containsExactlyInAnyOrderElementsOf(identifiers(first.tableFiles));

        store.commitData(
                Collections.singletonList(record("20260501", 8)), gen::getPartition, kv -> 0);
        Snapshot snapshot3 = store.snapshotManager().latestSnapshot();
        CommitChanges afterTargetAppend = provider.provide(snapshot3);

        assertThat(provider.fullScanManifestCount).isEqualTo(2);
        assertThat(provider.deltaProbeCount).isEqualTo(2);
        assertThat(identifiers(afterTargetAppend.tableFiles))
                .containsExactlyInAnyOrderElementsOf(
                        currentIdentifiers(store, snapshot3, partitionFilter));
        assertThat(afterTargetAppend.tableFiles).allMatch(entry -> entry.kind() == FileKind.DELETE);
    }

    @Test
    public void testIndexManifestEntriesCache() throws Exception {
        TestFileStore store = createStore(2, Collections.emptyMap());
        IndexFileHandler indexFileHandler = store.newIndexFileHandler();

        KeyValue targetRecord = record("20260501", 8);
        BinaryRow targetPartition = gen.getPartition(targetRecord);
        store.commitDataIndex(
                targetRecord,
                gen::getPartition,
                0,
                indexFileHandler.hashIndex(targetPartition, 0).write(new int[] {1, 2, 3}));
        Snapshot snapshot1 = store.snapshotManager().latestSnapshot();

        PartitionPredicate partitionFilter = partitionFilter(store, targetPartition);
        OverwriteChangesProvider provider =
                provider(store, partitionFilter, Collections.emptyList());

        CommitChanges first = provider.provide(snapshot1);
        assertThat(provider.fullScanIndexCount).isEqualTo(1);
        assertThat(first.indexFiles).hasSize(1);
        assertThat(first.indexFiles.get(0).kind()).isEqualTo(FileKind.DELETE);
        assertThat(first.indexFiles.get(0).partition()).isEqualTo(targetPartition);

        KeyValue unrelatedRecordWithIndex = record("20260502", 8);
        BinaryRow unrelatedPartition = gen.getPartition(unrelatedRecordWithIndex);
        store.commitDataIndex(
                unrelatedRecordWithIndex,
                gen::getPartition,
                0,
                indexFileHandler.hashIndex(unrelatedPartition, 0).write(new int[] {4, 5, 6}));
        Snapshot snapshot2 = store.snapshotManager().latestSnapshot();

        assertThat(snapshot2.indexManifest()).isNotEqualTo(snapshot1.indexManifest());
        CommitChanges afterUnrelatedIndexChange = provider.provide(snapshot2);
        assertThat(provider.fullScanManifestCount).isEqualTo(1);
        assertThat(provider.deltaProbeCount).isEqualTo(1);
        assertThat(provider.fullScanIndexCount).isEqualTo(2);
        assertThat(afterUnrelatedIndexChange.indexFiles)
                .containsExactlyInAnyOrderElementsOf(first.indexFiles);

        store.commitData(
                Collections.singletonList(record("20260502", 8)), gen::getPartition, kv -> 0);
        Snapshot snapshot3 = store.snapshotManager().latestSnapshot();

        assertThat(snapshot3.indexManifest()).isEqualTo(snapshot2.indexManifest());
        CommitChanges afterUnrelatedDataChange = provider.provide(snapshot3);
        assertThat(provider.fullScanManifestCount).isEqualTo(1);
        assertThat(provider.deltaProbeCount).isEqualTo(2);
        assertThat(provider.fullScanIndexCount).isEqualTo(2);
        assertThat(afterUnrelatedDataChange.indexFiles)
                .containsExactlyInAnyOrderElementsOf(first.indexFiles);
    }

    @Test
    public void testFullTableOverwriteSkipsDeltaProbe() throws Exception {
        TestFileStore store = createStore(1, Collections.emptyMap());
        store.commitData(
                Collections.singletonList(record("20260501", 8)), gen::getPartition, kv -> 0);
        Snapshot snapshot1 = store.snapshotManager().latestSnapshot();

        // partitionFilter == null means overwrite the whole table
        OverwriteChangesProvider provider = provider(store, null, Collections.emptyList());
        provider.provide(snapshot1);
        assertThat(provider.fullScanManifestCount).isEqualTo(1);
        assertThat(provider.deltaProbeCount).isZero();

        store.commitData(
                Collections.singletonList(record("20260502", 8)), gen::getPartition, kv -> 0);
        Snapshot snapshot2 = store.snapshotManager().latestSnapshot();
        provider.provide(snapshot2);

        // partitionFilter == null short-circuits delta probe and forces fullScan
        assertThat(provider.fullScanManifestCount).isEqualTo(2);
        assertThat(provider.deltaProbeCount).isZero();
    }

    @Test
    public void testExpiredSnapshotFallbacksToFullScan() throws Exception {
        TestFileStore store = createStore(1, Collections.emptyMap());
        KeyValue targetRecord = record("20260501", 8);
        BinaryRow targetPartition = gen.getPartition(targetRecord);
        store.commitData(
                Arrays.asList(targetRecord, record("20260502", 8)), gen::getPartition, kv -> 0);
        Snapshot snapshot1 = store.snapshotManager().latestSnapshot();

        PartitionPredicate partitionFilter = partitionFilter(store, targetPartition);
        OverwriteChangesProvider provider =
                provider(store, partitionFilter, Collections.emptyList());
        CommitChanges first = provider.provide(snapshot1);
        assertThat(provider.fullScanManifestCount).isEqualTo(1);

        store.commitData(
                Collections.singletonList(record("20260502", 8)), gen::getPartition, kv -> 0);
        Snapshot snapshot2 = store.snapshotManager().latestSnapshot();
        store.commitData(
                Collections.singletonList(record("20260502", 8)), gen::getPartition, kv -> 0);
        Snapshot snapshot3 = store.snapshotManager().latestSnapshot();

        // Simulate snapshot expiration of the middle snapshot.
        store.snapshotManager().deleteSnapshot(snapshot2.id());

        CommitChanges afterExpired = provider.provide(snapshot3);

        // Missing middle snapshot should not fail the commit; fall back to a full scan.
        assertThat(provider.fullScanManifestCount).isEqualTo(2);
        assertThat(identifiers(afterExpired.tableFiles))
                .containsExactlyInAnyOrderElementsOf(identifiers(first.tableFiles));
    }

    @Test
    public void testNonAppendSnapshotInvalidatesCache() throws Exception {
        TestFileStore store = createStore(1, Collections.emptyMap());
        KeyValue targetRecord = record("20260501", 8);
        BinaryRow targetPartition = gen.getPartition(targetRecord);
        store.commitData(
                Arrays.asList(targetRecord, record("20260502", 8)), gen::getPartition, kv -> 0);
        Snapshot snapshot1 = store.snapshotManager().latestSnapshot();

        PartitionPredicate partitionFilter = partitionFilter(store, targetPartition);
        OverwriteChangesProvider provider =
                provider(store, partitionFilter, Collections.emptyList());
        CommitChanges first = provider.provide(snapshot1);
        assertThat(provider.fullScanManifestCount).isEqualTo(1);

        // Overwrite an unrelated partition — commitKind != APPEND must invalidate the cache.
        Map<String, String> unrelated = new HashMap<>();
        unrelated.put("dt", "20260502");
        unrelated.put("hr", "8");
        store.overwriteData(
                Collections.singletonList(record("20260502", 8)),
                gen::getPartition,
                kv -> 0,
                unrelated);
        Snapshot snapshot2 = store.snapshotManager().latestSnapshot();
        CommitChanges afterOverwrite = provider.provide(snapshot2);
        assertThat(provider.fullScanManifestCount).isEqualTo(2);
        assertThat(provider.deltaProbeCount).isEqualTo(1);
        assertThat(identifiers(afterOverwrite.tableFiles))
                .containsExactlyInAnyOrderElementsOf(identifiers(first.tableFiles));

        // COMPACT snapshot also invalidates the cache.
        try (FileStoreCommitImpl commit = store.newCommit()) {
            commit.compactManifest();
        }
        Snapshot snapshot3 = store.snapshotManager().latestSnapshot();
        CommitChanges afterCompact = provider.provide(snapshot3);
        assertThat(provider.fullScanManifestCount).isEqualTo(3);
        assertThat(provider.deltaProbeCount).isEqualTo(2);
        assertThat(identifiers(afterCompact.tableFiles))
                .containsExactlyInAnyOrderElementsOf(identifiers(first.tableFiles));
    }

    private OverwriteChangesProvider provider(
            TestFileStore store,
            PartitionPredicate partitionFilter,
            List<ManifestEntry> newChanges) {
        return new OverwriteChangesProvider(
                store::newScan,
                store.snapshotManager(),
                store.indexManifestFileFactory().create(),
                store.options().manifestDeleteFileDropStats(),
                store.options().bucket(),
                newChanges,
                Collections.emptyList(),
                partitionFilter);
    }

    private PartitionPredicate partitionFilter(TestFileStore store, BinaryRow partition) {
        return PartitionPredicate.fromMultiple(
                store.partitionType(), Collections.singletonList(partition));
    }

    private List<FileEntry.Identifier> currentIdentifiers(
            TestFileStore store, Snapshot snapshot, PartitionPredicate partitionFilter) {
        return identifiers(
                store.newScan()
                        .withSnapshot(snapshot)
                        .withPartitionFilter(partitionFilter)
                        .plan()
                        .files());
    }

    private List<FileEntry.Identifier> identifiers(List<ManifestEntry> entries) {
        return entries.stream().map(ManifestEntry::identifier).collect(Collectors.toList());
    }

    private KeyValue record(String dt, int hr) {
        return gen.nextPartitionedData(RowKind.INSERT, dt, hr);
    }

    private TestFileStore createStore(int numBucket, Map<String, String> options) throws Exception {
        Path path = new Path(tempDir.toUri());
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(new LocalFileIO(), path),
                        new Schema(
                                TestKeyValueGenerator.DEFAULT_ROW_TYPE.getFields(),
                                TestKeyValueGenerator.DEFAULT_PART_TYPE.getFieldNames(),
                                TestKeyValueGenerator.getPrimaryKeys(
                                        TestKeyValueGenerator.GeneratorMode.MULTI_PARTITIONED),
                                options,
                                null));
        return new TestFileStore.Builder(
                        "avro",
                        TraceableFileIO.SCHEME + "://" + tempDir,
                        numBucket,
                        TestKeyValueGenerator.DEFAULT_PART_TYPE,
                        TestKeyValueGenerator.KEY_TYPE,
                        TestKeyValueGenerator.DEFAULT_ROW_TYPE,
                        TestKeyValueGenerator.TestKeyValueFieldsExtractor.EXTRACTOR,
                        DeduplicateMergeFunction.factory(),
                        tableSchema)
                .changelogProducer(CoreOptions.ChangelogProducer.NONE)
                .build();
    }
}
