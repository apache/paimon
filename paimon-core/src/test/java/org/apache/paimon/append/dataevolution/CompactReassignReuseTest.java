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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.RenamingSnapshotCommit;
import org.apache.paimon.catalog.SnapshotCommit;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.globalindex.DataEvolutionGlobalIndexScanner;
import org.apache.paimon.globalindex.GlobalIndexBuilderUtils;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexScanner;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexTestUtils;
import org.apache.paimon.index.DataEvolutionIndexSourceMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.operation.commit.CommitChanges;
import org.apache.paimon.operation.commit.DataEvolutionChangesProvider;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real compact outputs must survive only metadata-only reassignment conflicts. */
public class CompactReassignReuseTest extends TableTestBase {

    private final Map<CommitMessage, Long> lastSafeSnapshots = new IdentityHashMap<>();

    @Override
    protected Schema schemaDefault() {
        return Schema.newBuilder()
                .column("pt", DataTypes.STRING())
                .column("id", DataTypes.INT())
                .column("image", DataTypes.BLOB())
                .partitionKeys("pt")
                .option("bucket", "-1")
                .option("row-tracking.enabled", "true")
                .option("data-evolution.enabled", "true")
                .option("manifest.sidecar.enabled", "true")
                .option("snapshot.num-retained.min", "100")
                .build();
    }

    private FileStoreTable prepare() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        write(table, "a", 0, false);
        write(table, "a", 1, false);
        write(table, "b", 2, false);
        write(table, "a", 3, false);
        return table;
    }

    private void write(FileStoreTable table, String pt, int id, boolean overwrite)
            throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        if (overwrite) {
            builder.withOverwrite(Collections.singletonMap("pt", pt));
        }
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.write(
                    GenericRow.of(
                            BinaryString.fromString(pt),
                            id,
                            new BlobData(("image-" + id).getBytes(StandardCharsets.UTF_8))));
            commit.commit(write.prepareCommit());
        }
    }

    private CommitMessageImpl compact(FileStoreTable table, boolean blob) throws Exception {
        List<ManifestEntry> entries =
                table.store().newScan().plan().files().stream()
                        .filter(e -> e.partition().getString(0).toString().equals("a"))
                        .filter(e -> isBlobFile(e.file().fileName()) == blob)
                        .filter(e -> e.file().nonNullFirstRowId() < 2)
                        .sorted(Comparator.comparingLong(e -> e.file().nonNullFirstRowId()))
                        .collect(Collectors.toList());
        assertThat(entries).hasSize(2);
        List<DataFileMeta> files =
                entries.stream().map(ManifestEntry::file).collect(Collectors.toList());
        DataEvolutionCompactTask task =
                blob
                        ? new DataEvolutionBlobCompactTask(entries.get(0).partition(), files)
                        : new DataEvolutionNormalCompactTask(entries.get(0).partition(), files);
        CommitMessageImpl message = (CommitMessageImpl) task.doCompact(table, "compact-worker");
        assertThat(message.checkFromSnapshot()).isNull();
        lastSafeSnapshots.put(message, table.snapshotManager().latestSnapshotId());
        return message;
    }

    private List<CommitMessage> buildIndex(FileStoreTable table, String field, String partition)
            throws Exception {
        return buildIndex(table, "btree", field, partition);
    }

    private List<CommitMessage> buildIndex(
            FileStoreTable table, String indexType, String field, String partition)
            throws Exception {
        return buildIndex(table, indexType, Collections.singletonList(field), partition);
    }

    private List<CommitMessage> buildIndex(
            FileStoreTable table, String indexType, List<String> fields, String partition)
            throws Exception {
        List<CommitMessage> builds = new ArrayList<>();
        for (DataSplit split :
                GlobalIndexBuilderUtils.splitByContiguousRowRange(
                        new SortedGlobalIndexScanner(table, indexType)
                                .withIndexFields(fields)
                                .scan()
                                .get()
                                .entries())) {
            if (partition == null || split.partition().getString(0).toString().equals(partition)) {
                builds.addAll(
                        SortedGlobalIndexTestUtils.buildIndex(
                                table,
                                indexType,
                                fields,
                                split,
                                table.snapshotManager().latestSnapshotId()));
            }
        }
        return builds;
    }

    private CommitMessageImpl compactIndex(FileStoreTable table) throws Exception {
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(buildIndex(table, "id", null));
        }
        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        List<IndexManifestEntry> inputs =
                table.store().indexManifestFileFactory().create().read(snapshot.indexManifest())
                        .stream()
                        .filter(e -> e.partition().getString(0).toString().equals("a"))
                        .filter(
                                e ->
                                        e.indexFile()
                                                .globalIndexMeta()
                                                .rowRange()
                                                .equals(new Range(0, 1)))
                        .collect(Collectors.toList());
        assertThat(inputs).isNotEmpty();
        DataSplit split =
                GlobalIndexBuilderUtils.splitByContiguousRowRange(
                                new SortedGlobalIndexScanner(table, "btree")
                                        .withIndexFields(Collections.singletonList("id"))
                                        .scan()
                                        .get()
                                        .entries())
                        .stream()
                        .filter(s -> s.partition().equals(inputs.get(0).partition()))
                        .filter(
                                s ->
                                        s.dataFiles().stream()
                                                .allMatch(f -> f.nonNullFirstRowId() < 2))
                        .findFirst()
                        .get();
        List<IndexFileMeta> outputs = new ArrayList<>();
        for (CommitMessage built :
                SortedGlobalIndexTestUtils.buildIndex(table, "btree", "id", split, snapshot.id())) {
            outputs.addAll(((CommitMessageImpl) built).newFilesIncrement().newIndexFiles());
        }
        CommitMessageImpl message =
                new CommitMessageImpl(
                        inputs.get(0).partition(),
                        0,
                        null,
                        DataIncrement.emptyIncrement(),
                        new CompactIncrement(
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                outputs,
                                inputs.stream()
                                        .map(IndexManifestEntry::indexFile)
                                        .collect(Collectors.toList())));
        lastSafeSnapshots.put(message, snapshot.id());
        return message;
    }

    private void commit(FileStoreTable table, CommitMessage message, boolean strict)
            throws Exception {
        commit(table, message, strict, CoreOptions.COMMIT_LAST_SAFE_SNAPSHOT.key());
    }

    private void commit(FileStoreTable table, CommitMessage message, boolean strict, String key)
            throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(key, lastSafeSnapshots.get(message).toString());
        options.put(CoreOptions.COMMIT_STRICT_MODE_ENABLED.key(), Boolean.toString(strict));
        try (BatchTableCommit commit = table.copy(options).newBatchWriteBuilder().newCommit()) {
            commit.commit(Collections.singletonList(message));
        }
    }

    private void checkRows(FileStoreTable table, int... expected) throws Exception {
        ReadBuilder builder = table.newReadBuilder();
        TreeSet<Integer> actual = new TreeSet<>();
        try (RecordReader<InternalRow> reader =
                        builder.newRead().createReader(builder.newScan().plan());
                CloseableIterator<InternalRow> rows = reader.toCloseableIterator()) {
            while (rows.hasNext()) {
                InternalRow row = rows.next();
                int id = row.getInt(1);
                assertThat(actual.add(id)).isTrue();
                assertThat(row.getBlob(2).toData())
                        .isEqualTo(("image-" + id).getBytes(StandardCharsets.UTF_8));
            }
        }
        assertThat(actual)
                .containsExactlyElementsOf(
                        Arrays.stream(expected).boxed().collect(Collectors.toList()));
    }

    @ParameterizedTest
    @CsvSource({
        "btree,false,after-scan", "btree,true,after-scan",
        "bitmap,false,after-scan", "bitmap,true,after-scan",
        "composite,false,after-scan", "composite,true,after-scan",
        "btree,false,before-scan", "btree,true,before-scan",
        "bitmap,false,before-scan", "bitmap,true,before-scan",
        "composite,false,before-scan", "composite,true,before-scan",
        "btree,false,index-first", "btree,true,index-first",
        "bitmap,false,index-first", "bitmap,true,index-first",
        "composite,false,index-first", "composite,true,index-first"
    })
    void testIndexBuildAcrossReassignment(String indexType, boolean strict, String order)
            throws Exception {
        FileStoreTable table = prepare();
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(buildIndex(table, "pt", null));
        }
        long lastSafeSnapshot = table.snapshotManager().latestSnapshotId();
        boolean reassignBeforeScan = order.equals("before-scan");
        if (reassignBeforeScan) {
            assertThat(new DataEvolutionRowIdReassigner(table).reassign().reassigned).isTrue();
        }
        long scanSnapshot = table.snapshotManager().latestSnapshotId();
        boolean composite = indexType.equals("composite");
        List<CommitMessage> messages =
                buildIndex(
                        table,
                        composite ? "btree" : indexType,
                        composite ? Arrays.asList("id", "pt") : Collections.singletonList("id"),
                        null);
        List<String> files =
                messages.stream()
                        .flatMap(
                                m ->
                                        ((CommitMessageImpl) m)
                                                .newFilesIncrement().newIndexFiles().stream())
                        .map(IndexFileMeta::fileName)
                        .collect(Collectors.toList());
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.COMMIT_STRICT_MODE_ENABLED.key(), Boolean.toString(strict));
        options.put(CoreOptions.COMMIT_LAST_SAFE_SNAPSHOT.key(), Long.toString(lastSafeSnapshot));
        FileStoreTable committingTable = table.copy(options);
        AtomicBoolean indexCommitted = new AtomicBoolean();
        Runnable commitIndex =
                () -> {
                    if (indexCommitted.compareAndSet(false, true)) {
                        try (BatchTableCommit commit =
                                committingTable.newBatchWriteBuilder().newCommit()) {
                            commit.commit(messages);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                };
        boolean indexFirst = order.equals("index-first");
        if (indexFirst) {
            DataEvolutionRowIdReassigner.Result result =
                    new DataEvolutionRowIdReassigner(table, null, commitIndex).reassign();
            assertThat(result.reassigned).isTrue();
            assertThat(result.previousSnapshotId).isEqualTo(scanSnapshot + 1);
        } else {
            if (!reassignBeforeScan) {
                assertThat(new DataEvolutionRowIdReassigner(table).reassign().reassigned).isTrue();
            }
            commitIndex.run();
        }
        assertThat(indexCommitted).isTrue();
        assertThat(table.snapshotManager().latestSnapshot().commitKind())
                .isEqualTo(indexFirst ? Snapshot.CommitKind.OVERWRITE : Snapshot.CommitKind.APPEND);
        List<IndexManifestEntry> current =
                table.store().newIndexFileHandler().scanEntries().stream()
                        .filter(e -> e.indexFile().globalIndexMeta().indexFieldId() == 1)
                        .collect(Collectors.toList());
        assertThat(current)
                .extracting(e -> e.indexFile().fileName())
                .containsExactlyInAnyOrderElementsOf(files);
        for (IndexManifestEntry entry : current) {
            assertThat(
                            DataEvolutionIndexSourceMeta.fromIndexFile(entry.indexFile())
                                    .scanSnapshotId())
                    .isEqualTo(scanSnapshot);
        }
        long[] rowIds = {4, 5, 2, 6};
        for (int id = 0; id < rowIds.length; id++) {
            PredicateBuilder builder = new PredicateBuilder(table.rowType());
            Predicate predicate = builder.equal(1, id);
            if (composite) {
                predicate =
                        PredicateBuilder.and(
                                predicate,
                                builder.equal(0, BinaryString.fromString(id == 2 ? "b" : "a")));
            }
            try (DataEvolutionGlobalIndexScanner scanner =
                    DataEvolutionGlobalIndexScanner.create(table, null, predicate).get()) {
                GlobalIndexResult result = scanner.scan(predicate).get();
                assertThat(result.results().getLongCardinality()).isEqualTo(1);
                assertThat(result.results().contains(rowIds[id])).isTrue();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testIndexBuildKeepsExistingColumnUpdateBehavior(boolean reassign) throws Exception {
        FileStoreTable table = prepare();
        long scanSnapshot = table.snapshotManager().latestSnapshotId();
        List<CommitMessage> messages = buildIndex(table, "id", null);
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write =
                        builder.newWrite()
                                .withWriteType(table.rowType().project(Arrays.asList("pt", "id")));
                BatchTableCommit commit = builder.newCommit()) {
            write.write(GenericRow.of(BinaryString.fromString("a"), 100));
            List<CommitMessage> updates = write.prepareCommit();
            for (CommitMessage update : updates) {
                ((CommitMessageImpl) update)
                        .newFilesIncrement()
                        .newFiles()
                        .replaceAll(file -> file.assignFirstRowId(0L));
            }
            commit.commit(updates);
        }
        if (reassign) {
            new DataEvolutionRowIdReassigner(table).reassign();
        }
        long snapshot = table.snapshotManager().latestSnapshotId();
        try (BatchTableCommit commit =
                table.copy(
                                Collections.singletonMap(
                                        CoreOptions.COMMIT_LAST_SAFE_SNAPSHOT.key(),
                                        Long.toString(scanSnapshot)))
                        .newBatchWriteBuilder()
                        .newCommit()) {
            commit.commit(messages);
        }
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(snapshot + 1);
        assertThat(read(table))
                .extracting(row -> row.getInt(1))
                .containsExactlyInAnyOrder(100, 1, 2, 3);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testReuseNormalAndBlobAcrossRepeatedReassignments(boolean blob) throws Exception {
        FileStoreTable table = prepare();
        if (blob) {
            // A merged BLOB must stay within one normal-file range.
            commit(table, compact(table, false), false);
        }
        CommitMessageImpl message = compact(table, blob);
        List<String> outputNames =
                message.compactIncrement().compactAfter().stream()
                        .map(DataFileMeta::fileName)
                        .collect(Collectors.toList());
        assertThat(new DataEvolutionRowIdReassigner(table).reassign().reassigned).isTrue();
        write(table, "b", 4, false);
        write(table, "a", 5, false);
        assertThat(new DataEvolutionRowIdReassigner(table).reassign().reassigned).isTrue();
        commit(table, message, true);
        assertThat(table.snapshotManager().latestSnapshot().commitKind())
                .isEqualTo(Snapshot.CommitKind.COMPACT);
        List<DataFileMeta> outputs =
                table.store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .filter(f -> outputNames.contains(f.fileName()))
                        .collect(Collectors.toList());
        assertThat(outputs).hasSize(outputNames.size());
        assertThat(outputs)
                .allSatisfy(f -> assertThat(f.nonNullFirstRowId()).isGreaterThanOrEqualTo(9));
        // The original worker result is reusable, not mutated in-place on each retry.
        assertThat(message.compactIncrement().compactAfter().get(0).nonNullFirstRowId()).isZero();
        checkRows(table, 0, 1, 2, 3, 4, 5);
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void testIndexReplacementKeepsPhysicalFilesAndPointLookup(
            boolean indexBuild, boolean loseSuccessfulResponse) throws Exception {
        FileStoreTable table = prepare();
        CommitMessageImpl message = compactIndex(table);
        List<IndexFileMeta> outputs = message.compactIncrement().newIndexFiles();
        List<String> replacedFiles =
                message.compactIncrement().deletedIndexFiles().stream()
                        .map(IndexFileMeta::fileName)
                        .collect(Collectors.toList());
        long scanSnapshot = lastSafeSnapshots.get(message);
        if (indexBuild) {
            message =
                    new CommitMessageImpl(
                            message.partition(),
                            message.bucket(),
                            message.totalBuckets(),
                            new DataIncrement(
                                    Collections.emptyList(),
                                    Collections.emptyList(),
                                    Collections.emptyList(),
                                    outputs,
                                    message.compactIncrement().deletedIndexFiles()),
                            CompactIncrement.emptyIncrement());
            lastSafeSnapshots.put(message, scanSnapshot);
        }
        assertThat(new DataEvolutionRowIdReassigner(table).reassign().reassigned).isTrue();
        long reassignedSnapshot = table.snapshotManager().latestSnapshotId();
        AtomicInteger attempts = new AtomicInteger();
        CatalogEnvironment environment =
                new CatalogEnvironment(null, null, null, null, null, null, false, false) {
                    @Override
                    public SnapshotCommit snapshotCommit(
                            SnapshotManager manager, CoreOptions options, String commitUser) {
                        SnapshotCommit delegate = new RenamingSnapshotCommit(manager, Lock.empty());
                        return new SnapshotCommit(Lock.empty()) {
                            @Override
                            protected boolean commitImpl(
                                    String baseUuid,
                                    Snapshot snapshot,
                                    String branch,
                                    List<PartitionStatistics> statistics)
                                    throws Exception {
                                boolean committed =
                                        delegate.commit(baseUuid, snapshot, branch, statistics);
                                if (snapshot.commitKind()
                                        == (indexBuild
                                                ? Snapshot.CommitKind.APPEND
                                                : Snapshot.CommitKind.COMPACT)) {
                                    attempts.incrementAndGet();
                                    return committed && !loseSuccessfulResponse;
                                }
                                return committed;
                            }

                            @Override
                            protected void closeImpl() throws Exception {
                                delegate.close();
                            }
                        };
                    }
                };
        FileStoreTable committingTable =
                FileStoreTableFactory.create(
                        table.fileIO(), table.location(), table.schema(), environment);
        if (indexBuild) {
            // A replaced index's source baseline predates this refresh's planning snapshot.
            assertThat(
                            table.fileIO()
                                    .delete(
                                            table.snapshotManager().snapshotPath(scanSnapshot),
                                            false))
                    .isTrue();
            table.snapshotManager().invalidateCache();
        }
        commit(committingTable, message, false);
        assertThat(attempts).hasValue(1);
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(reassignedSnapshot + 1);
        List<IndexManifestEntry> current =
                table.store()
                        .indexManifestFileFactory()
                        .create()
                        .read(table.snapshotManager().latestSnapshot().indexManifest());
        assertThat(current.stream().map(e -> e.indexFile().fileName()))
                .containsAll(
                        outputs.stream().map(IndexFileMeta::fileName).collect(Collectors.toList()))
                .doesNotContainAnyElementsOf(replacedFiles);
        Predicate predicate = new PredicateBuilder(table.rowType()).equal(1, 1);
        try (DataEvolutionGlobalIndexScanner scanner =
                DataEvolutionGlobalIndexScanner.create(table, null, predicate).get()) {
            GlobalIndexResult ids = scanner.scan(predicate).get();
            assertThat(ids.results().getLongCardinality()).isEqualTo(1);
            assertThat(ids.results().contains(5L)).isTrue();
        }
        checkRows(table, 0, 1, 2, 3);
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void testLostIndexCommitResponseBeforeDeletionAndReassignment(
            boolean reassignBeforeCompact, boolean throwAfterSuccess) throws Exception {
        FileStoreTable table = prepare();
        CommitMessageImpl message = compactIndex(table);
        if (reassignBeforeCompact) {
            assertThat(new DataEvolutionRowIdReassigner(table).reassign().reassigned).isTrue();
        }
        List<String> outputNames =
                message.compactIncrement().newIndexFiles().stream()
                        .map(IndexFileMeta::fileName)
                        .collect(Collectors.toList());
        AtomicInteger attempts = new AtomicInteger();
        AtomicReference<Snapshot> successfulSnapshot = new AtomicReference<>();
        AtomicReference<Snapshot> reassignedSnapshot = new AtomicReference<>();
        CatalogEnvironment environment =
                new CatalogEnvironment(null, null, null, null, null, null, false, false) {
                    @Override
                    public SnapshotCommit snapshotCommit(
                            SnapshotManager manager, CoreOptions options, String commitUser) {
                        SnapshotCommit delegate = new RenamingSnapshotCommit(manager, Lock.empty());
                        return new SnapshotCommit(Lock.empty()) {
                            @Override
                            protected boolean commitImpl(
                                    String baseUuid,
                                    Snapshot snapshot,
                                    String branch,
                                    List<PartitionStatistics> statistics)
                                    throws Exception {
                                boolean committed =
                                        delegate.commit(baseUuid, snapshot, branch, statistics);
                                if (snapshot.commitKind() != Snapshot.CommitKind.COMPACT) {
                                    return committed;
                                }
                                attempts.incrementAndGet();
                                assertThat(committed).isTrue();
                                successfulSnapshot.set(snapshot);
                                List<IndexManifestEntry> outputs =
                                        table.store().indexManifestFileFactory().create()
                                                .read(snapshot.indexManifest()).stream()
                                                .filter(
                                                        e ->
                                                                outputNames.contains(
                                                                        e.indexFile().fileName()))
                                                .collect(Collectors.toList());
                                assertThat(outputs).hasSize(outputNames.size());
                                Range obsoleteRange =
                                        outputs.get(0).indexFile().globalIndexMeta().rowRange();
                                List<DataFileMeta> deleted =
                                        table.store().newScan().withSnapshot(snapshot).plan()
                                                .files().stream()
                                                .filter(
                                                        e ->
                                                                e.partition()
                                                                        .equals(
                                                                                message
                                                                                        .partition()))
                                                .map(ManifestEntry::file)
                                                .filter(
                                                        f ->
                                                                f.nonNullFirstRowId()
                                                                        == obsoleteRange.from)
                                                .collect(Collectors.toList());
                                assertThat(deleted).hasSize(2);
                                assertThat(deleted)
                                        .allSatisfy(f -> assertThat(f.rowCount()).isEqualTo(1));
                                try (BatchTableCommit deletion =
                                        table.newBatchWriteBuilder().newCommit()) {
                                    deletion.commit(
                                            Collections.singletonList(
                                                    new CommitMessageImpl(
                                                            message.partition(),
                                                            message.bucket(),
                                                            null,
                                                            new DataIncrement(
                                                                    Collections.emptyList(),
                                                                    deleted,
                                                                    Collections.emptyList()),
                                                            CompactIncrement.emptyIncrement())));
                                }
                                // Keep the surviving partition ranges fragmented so reassignment
                                // runs.
                                write(table, "b", 4, false);
                                write(table, "a", 5, false);
                                assertThat(
                                                new DataEvolutionRowIdReassigner(table)
                                                        .reassign()
                                                        .reassigned)
                                        .isTrue();
                                Snapshot latest = table.snapshotManager().latestSnapshot();
                                reassignedSnapshot.set(latest);
                                String plan = SerializationAssignment.planFile(latest);
                                assertThat(plan).isNotNull();
                                SerializationAssignment assignment =
                                        SerializationAssignment.readPlan(
                                                table.fileIO(), table.store().pathFactory(), plan);
                                assertThatThrownBy(
                                                () ->
                                                        assignment.mapRowRange(
                                                                message.partition(), obsoleteRange))
                                        .hasMessageContaining(
                                                "only partially or non-contiguously mapped");
                                assertThat(
                                                table.store().indexManifestFileFactory().create()
                                                        .read(latest.indexManifest()).stream()
                                                        .map(e -> e.indexFile().fileName()))
                                        .doesNotContainAnyElementsOf(outputNames);
                                if (throwAfterSuccess) {
                                    throw new IOException(
                                            "Lost successful index compaction response");
                                }
                                return false;
                            }

                            @Override
                            protected void closeImpl() throws Exception {
                                delegate.close();
                            }
                        };
                    }
                };
        FileStoreTable committingTable =
                FileStoreTableFactory.create(
                        table.fileIO(), table.location(), table.schema(), environment);
        commit(committingTable, message, true);
        assertThat(attempts).hasValue(1);
        assertThat(successfulSnapshot.get().commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);
        assertThat(table.snapshotManager().latestSnapshotId())
                .isEqualTo(reassignedSnapshot.get().id());
        checkRows(table, 1, 2, 3, 4, 5);
    }

    @Test
    void testReuseCompactAfterOverwriteOtherPartition() throws Exception {
        FileStoreTable table = prepare();
        CommitMessageImpl message = compact(table, false);
        assertThat(new DataEvolutionRowIdReassigner(table).reassign().reassigned).isTrue();
        long reassignedSnapshot = table.snapshotManager().latestSnapshotId();

        BatchWriteBuilder builder =
                table.copy(
                                Collections.singletonMap(
                                        CoreOptions.COMMIT_LAST_SAFE_SNAPSHOT.key(),
                                        lastSafeSnapshots.get(message).toString()))
                        .newBatchWriteBuilder()
                        .withOverwrite(Collections.singletonMap("pt", "b"));
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.write(
                    GenericRow.of(
                            BinaryString.fromString("b"),
                            9,
                            new BlobData("image-9".getBytes(StandardCharsets.UTF_8))));
            List<CommitMessage> messages = new ArrayList<>(write.prepareCommit());
            messages.add(message);
            commit.commit(messages);
        }

        Snapshot latest = table.snapshotManager().latestSnapshot();
        assertThat(latest.id()).isEqualTo(reassignedSnapshot + 2);
        assertThat(latest.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);
        assertThat(message.compactIncrement().compactAfter().get(0).nonNullFirstRowId()).isZero();
        checkRows(table, 0, 1, 3, 9);
    }

    @Test
    void testRealOverwriteStillConflicts() throws Exception {
        FileStoreTable table = prepare();
        CommitMessageImpl message = compact(table, false);
        assertThat(new DataEvolutionRowIdReassigner(table).reassign().reassigned).isTrue();
        write(table, "a", 9, true);
        long snapshot = table.snapshotManager().latestSnapshotId();
        assertThatThrownBy(() -> commit(table, message, false)).hasStackTraceContaining("conflict");
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(snapshot);
        checkRows(table, 2, 9);
    }

    @Test
    void testReplacedInputsStillConflict() throws Exception {
        FileStoreTable table = prepare();
        CommitMessageImpl first = compact(table, false);
        CommitMessageImpl second = compact(table, false);
        commit(table, first, false);
        long snapshot = table.snapshotManager().latestSnapshotId();
        assertThatThrownBy(() -> commit(table, second, false)).hasStackTraceContaining("conflict");
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(snapshot);
        checkRows(table, 0, 1, 2, 3);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testMissingPlanFailsClosed(boolean indexBuild) throws Exception {
        FileStoreTable table = prepare();
        CommitMessageImpl message =
                indexBuild
                        ? (CommitMessageImpl) buildIndex(table, "id", "a").get(0)
                        : compact(table, false);
        lastSafeSnapshots.put(message, table.snapshotManager().latestSnapshotId());
        new DataEvolutionRowIdReassigner(table).reassign();
        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        Path plan =
                table.store()
                        .pathFactory()
                        .toManifestFilePath(SerializationAssignment.planFile(snapshot));
        table.fileIO().deleteQuietly(plan);
        assertThatThrownBy(() -> commit(table, message, false))
                .hasStackTraceContaining("Cannot read compaction reassignment plan");
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(snapshot.id());
        checkRows(table, 0, 1, 2, 3);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCorruptPlanFailsClosed(boolean indexBuild) throws Exception {
        FileStoreTable table = prepare();
        CommitMessageImpl message =
                indexBuild
                        ? (CommitMessageImpl) buildIndex(table, "id", "a").get(0)
                        : compact(table, false);
        lastSafeSnapshots.put(message, table.snapshotManager().latestSnapshotId());
        new DataEvolutionRowIdReassigner(table).reassign();
        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        Path path =
                table.store()
                        .pathFactory()
                        .toManifestFilePath(SerializationAssignment.planFile(snapshot));
        try (PositionOutputStream out = table.fileIO().newOutputStream(path, true)) {
            out.write(new byte[16]);
        }
        assertThatThrownBy(() -> commit(table, message, false)).hasStackTraceContaining("checksum");
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(snapshot.id());
        checkRows(table, 0, 1, 2, 3);
    }

    @ParameterizedTest
    @CsvSource({"false,", "true,", "false,-1", "true,-1"})
    void testNoBoundaryKeepsExistingConflictCheck(boolean indexBuild, Long lastSafeSnapshot)
            throws Exception {
        FileStoreTable table = prepare();
        CommitMessageImpl message =
                indexBuild
                        ? (CommitMessageImpl) buildIndex(table, "id", "a").get(0)
                        : compact(table, false);
        new DataEvolutionRowIdReassigner(table).reassign();
        long snapshot = table.snapshotManager().latestSnapshotId();
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.COMMIT_STRICT_MODE_ENABLED.key(), "false");
        if (lastSafeSnapshot != null) {
            options.put(CoreOptions.COMMIT_LAST_SAFE_SNAPSHOT.key(), lastSafeSnapshot.toString());
        }
        try (BatchTableCommit commit = table.copy(options).newBatchWriteBuilder().newCommit()) {
            assertThatThrownBy(() -> commit.commit(Collections.singletonList(message)))
                    .hasStackTraceContaining(
                            indexBuild
                                    ? "Global index row ID existence conflict"
                                    : "Row ID existence conflict");
        }
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(snapshot);
        checkRows(table, 0, 1, 2, 3);
    }

    @ParameterizedTest
    @ValueSource(strings = {"snapshot", "manifest-list"})
    void testIndexBuildMissingMetadataFailsClosed(String missing) throws Exception {
        FileStoreTable table = prepare();
        CommitMessageImpl message = (CommitMessageImpl) buildIndex(table, "id", "a").get(0);
        long boundary = table.snapshotManager().latestSnapshotId();
        lastSafeSnapshots.put(message, boundary);
        write(table, "b", 4, false);
        new DataEvolutionRowIdReassigner(table).reassign();
        Snapshot latest = table.snapshotManager().latestSnapshot();
        boolean snapshotMissing = missing.equals("snapshot");
        Path path =
                snapshotMissing
                        ? table.snapshotManager().snapshotPath(boundary + 1)
                        : table.store().pathFactory().toManifestListPath(latest.baseManifestList());
        assertThat(table.fileIO().delete(path, false)).isTrue();
        assertThatThrownBy(() -> commit(table, message, false))
                .hasStackTraceContaining(
                        snapshotMissing ? "missing snapshot" : latest.baseManifestList());
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(latest.id());
        assertThat(table.snapshotManager().snapshotExists(latest.id() + 1)).isFalse();
    }

    @Test
    void testExpiredBoundaryAndLegacyOption() throws Exception {
        FileStoreTable table = prepare();
        CommitMessageImpl message = compact(table, false);
        new DataEvolutionRowIdReassigner(table).reassign();
        table.fileIO()
                .deleteQuietly(
                        table.snapshotManager().snapshotPath(lastSafeSnapshots.get(message)));
        commit(table, message, true, "commit.strict-mode.last-safe-snapshot");
        checkRows(table, 0, 1, 2, 3);
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void testReassignDuringCommitRetries(boolean indexBuild, boolean loseSuccessfulResponse)
            throws Exception {
        FileStoreTable original = prepare();
        CommitMessageImpl message =
                indexBuild
                        ? (CommitMessageImpl) buildIndex(original, "id", "a").get(0)
                        : compact(original, false);
        lastSafeSnapshots.put(message, original.snapshotManager().latestSnapshotId());
        AtomicInteger attempts = new AtomicInteger();
        CatalogEnvironment environment =
                new CatalogEnvironment(null, null, null, null, null, null, false, false) {
                    @Override
                    public SnapshotCommit snapshotCommit(
                            SnapshotManager manager, CoreOptions options, String commitUser) {
                        SnapshotCommit delegate = new RenamingSnapshotCommit(manager, Lock.empty());
                        return new SnapshotCommit(Lock.empty()) {
                            @Override
                            protected boolean commitImpl(
                                    String baseUuid,
                                    Snapshot snapshot,
                                    String branch,
                                    List<PartitionStatistics> statistics)
                                    throws Exception {
                                if (snapshot.commitKind()
                                        != (indexBuild
                                                ? Snapshot.CommitKind.APPEND
                                                : Snapshot.CommitKind.COMPACT)) {
                                    return delegate.commit(baseUuid, snapshot, branch, statistics);
                                }
                                int attempt = attempts.getAndIncrement();
                                if (attempt == 0) {
                                    assertThat(
                                                    new DataEvolutionRowIdReassigner(original)
                                                            .reassign()
                                                            .reassigned)
                                            .isTrue();
                                } else if (attempt == 1) {
                                    write(original, "b", 4, false);
                                    write(original, "a", 5, false);
                                    assertThat(
                                                    new DataEvolutionRowIdReassigner(original)
                                                            .reassign()
                                                            .reassigned)
                                            .isTrue();
                                }
                                boolean committed =
                                        delegate.commit(baseUuid, snapshot, branch, statistics);
                                return committed && !loseSuccessfulResponse;
                            }

                            @Override
                            protected void closeImpl() throws Exception {
                                delegate.close();
                            }
                        };
                    }
                };
        FileStoreTable table =
                FileStoreTableFactory.create(
                        original.fileIO(), original.location(), original.schema(), environment);
        commit(table, message, true);
        assertThat(attempts).hasValue(3);
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(9);
        checkRows(table, 0, 1, 2, 3, 4, 5);
        if (indexBuild) {
            IndexFileMeta built = message.newFilesIncrement().newIndexFiles().get(0);
            assertThat(built.globalIndexMeta().rowRange()).isEqualTo(new Range(0, 1));
            assertThat(table.store().newIndexFileHandler().scanEntries())
                    .singleElement()
                    .satisfies(
                            entry -> {
                                assertThat(entry.indexFile().fileName())
                                        .isEqualTo(built.fileName());
                                assertThat(entry.indexFile().globalIndexMeta().rowRange())
                                        .isEqualTo(new Range(9, 10));
                            });
        } else {
            assertThat(message.compactIncrement().compactAfter().get(0).nonNullFirstRowId())
                    .isZero();
        }
    }

    @Test
    void testNoReassignmentDoesNotValidateMappingInputs() throws Exception {
        FileStoreTable table = prepare();
        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        ManifestEntry entry = table.store().newScan().plan().files().get(0);
        IndexManifestEntry unsupported =
                new IndexManifestEntry(
                        FileKind.ADD,
                        entry.partition(),
                        entry.bucket(),
                        new IndexFileMeta("unknown", "unsupported.index", 1, 1, null, null, null));
        CommitChanges changes =
                new CommitChanges(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonList(unsupported));
        DataEvolutionChangesProvider provider =
                new DataEvolutionChangesProvider(
                        table.fileIO(),
                        table.store().pathFactory(),
                        table.snapshotManager(),
                        snapshot.id(),
                        changes);

        write(table, "b", 4, false);
        assertThat(provider.provide(table.snapshotManager().latestSnapshot())).isSameAs(changes);
        assertThat(provider.rebasedReassignments()).isEmpty();
    }

    @ParameterizedTest
    @CsvSource({
        "missing-row-id, has no first row ID",
        "physical-row-id, physically stores _ROW_ID",
        "changelog, with changelog files",
        "unknown-index, unsupported index type"
    })
    void testUnsafeMappingFailsBeforeCommit(String kind, String expectedMessage) throws Exception {
        FileStoreTable table = prepare();
        ManifestEntry entry =
                table.store().newScan().plan().files().stream()
                        .filter(e -> e.file().nonNullFirstRowId() == 0)
                        .findFirst()
                        .get();
        DataFileMeta file =
                DataFileMeta.forAppend(
                        "unsupported.parquet",
                        1,
                        1,
                        SimpleStats.EMPTY_STATS,
                        0,
                        0,
                        table.schema().id(),
                        Collections.emptyList(),
                        null,
                        null,
                        null,
                        null,
                        kind.equals("missing-row-id") ? null : 0L,
                        kind.equals("physical-row-id")
                                ? Collections.singletonList(SpecialFields.ROW_ID.name())
                                : null);
        List<DataFileMeta> dataFiles =
                kind.equals("missing-row-id") || kind.equals("physical-row-id")
                        ? Collections.singletonList(file)
                        : Collections.emptyList();
        List<DataFileMeta> changelogFiles =
                kind.equals("changelog")
                        ? Collections.singletonList(file)
                        : Collections.emptyList();
        List<IndexFileMeta> indexFiles =
                kind.equals("unknown-index")
                        ? Collections.singletonList(
                                new IndexFileMeta(
                                        "unknown", "unsupported.index", 1, 1, null, null, null))
                        : Collections.emptyList();
        CommitMessageImpl message =
                new CommitMessageImpl(
                        entry.partition(),
                        entry.bucket(),
                        null,
                        DataIncrement.emptyIncrement(),
                        new CompactIncrement(
                                Collections.emptyList(),
                                dataFiles,
                                changelogFiles,
                                indexFiles,
                                Collections.emptyList()));
        lastSafeSnapshots.put(message, table.snapshotManager().latestSnapshotId());
        assertThat(new DataEvolutionRowIdReassigner(table).reassign().reassigned).isTrue();
        long snapshot = table.snapshotManager().latestSnapshotId();

        assertThatThrownBy(() -> commit(table, message, false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot reuse compaction across reassignment")
                .hasMessageContaining(expectedMessage);
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(snapshot);
        checkRows(table, 0, 1, 2, 3);
    }

    @Test
    void testNonContiguousMappingCannotReuseAFile() throws Exception {
        FileStoreTable table = prepare();
        ManifestEntry entry =
                table.store().newScan().plan().files().stream()
                        .filter(e -> e.partition().getString(0).toString().equals("a"))
                        .findFirst()
                        .get();
        RowRangeMappingIndex mapping =
                RowRangeMappingIndex.create(
                        Arrays.asList(
                                RowRangeMappingIndex.mapping(0, 0, 4),
                                RowRangeMappingIndex.mapping(1, 1, 7)));
        Map<String, String> properties =
                SerializationAssignment.writeProperties(
                        table,
                        table.snapshotManager().latestSnapshot(),
                        Collections.singletonMap(entry.partition(), mapping),
                        4,
                        8);
        SerializationAssignment assignment =
                SerializationAssignment.readPlan(
                        table.fileIO(),
                        table.store().pathFactory(),
                        properties.get(SerializationAssignment.PLAN_FILE_PROPERTY));
        assertThatThrownBy(() -> assignment.mapRowRange(entry.partition(), new Range(0, 1)))
                .hasMessageContaining("non-contiguously mapped");
        assertThatThrownBy(() -> assignment.mapRowRange(entry.partition(), new Range(0, 2)))
                .hasMessageContaining("non-contiguously mapped");
        assertThat(assignment.mapRowRange(entry.partition(), new Range(10, 11)))
                .isEqualTo(new Range(10, 11));
    }
}
