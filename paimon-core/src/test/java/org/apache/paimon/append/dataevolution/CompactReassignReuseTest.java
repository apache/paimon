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
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
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
import org.junit.jupiter.params.provider.ValueSource;

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
import java.util.concurrent.atomic.AtomicInteger;
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

    @Test
    void testIndexReplacementKeepsPhysicalFilesAndPointLookup() throws Exception {
        FileStoreTable table = prepare();
        List<CommitMessage> builds = new ArrayList<>();
        for (DataSplit split :
                GlobalIndexBuilderUtils.splitByContiguousRowRange(
                        new SortedGlobalIndexScanner(table, "btree")
                                .withIndexField("id")
                                .scan()
                                .get()
                                .entries())) {
            builds.addAll(SortedGlobalIndexTestUtils.buildIndex(table, "btree", "id", split, 4));
        }
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(builds);
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
                                        .withIndexField("id")
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
        assertThat(new DataEvolutionRowIdReassigner(table).reassign().reassigned).isTrue();
        commit(table, message, false);
        List<IndexManifestEntry> current =
                table.store()
                        .indexManifestFileFactory()
                        .create()
                        .read(table.snapshotManager().latestSnapshot().indexManifest());
        assertThat(current.stream().map(e -> e.indexFile().fileName()))
                .containsAll(
                        outputs.stream().map(IndexFileMeta::fileName).collect(Collectors.toList()));
        Predicate predicate = new PredicateBuilder(table.rowType()).equal(1, 1);
        try (DataEvolutionGlobalIndexScanner scanner =
                DataEvolutionGlobalIndexScanner.create(table, null, predicate).get()) {
            GlobalIndexResult ids = scanner.scan(predicate).get();
            assertThat(ids.results().getLongCardinality()).isEqualTo(1);
            assertThat(ids.results().contains(5L)).isTrue();
        }
        checkRows(table, 0, 1, 2, 3);
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

    @Test
    void testMissingPlanFailsClosed() throws Exception {
        FileStoreTable table = prepare();
        CommitMessageImpl message = compact(table, false);
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

    @Test
    void testCorruptPlanFailsClosed() throws Exception {
        FileStoreTable table = prepare();
        CommitMessageImpl message = compact(table, false);
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

    @Test
    void testNoBoundaryKeepsExistingConflictCheck() throws Exception {
        FileStoreTable table = prepare();
        CommitMessageImpl message = compact(table, false);
        new DataEvolutionRowIdReassigner(table).reassign();
        long snapshot = table.snapshotManager().latestSnapshotId();
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            assertThatThrownBy(() -> commit.commit(Collections.singletonList(message)))
                    .hasStackTraceContaining("conflict");
        }
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(snapshot);
        checkRows(table, 0, 1, 2, 3);
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
    @ValueSource(booleans = {false, true})
    void testReassignDuringCommitRetries(boolean loseSuccessfulResponse) throws Exception {
        FileStoreTable original = prepare();
        CommitMessageImpl message = compact(original, false);
        AtomicInteger attempts = new AtomicInteger();
        CatalogEnvironment environment =
                new CatalogEnvironment(null, null, null, null, null, null, false, false) {
                    @Override
                    public SnapshotCommit snapshotCommit(SnapshotManager manager) {
                        SnapshotCommit delegate = new RenamingSnapshotCommit(manager, Lock.empty());
                        return new SnapshotCommit() {
                            @Override
                            public boolean commit(
                                    String baseUuid,
                                    Snapshot snapshot,
                                    String branch,
                                    List<PartitionStatistics> statistics)
                                    throws Exception {
                                if (snapshot.commitKind() != Snapshot.CommitKind.COMPACT) {
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
                            public void close() throws Exception {
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
        assertThat(message.compactIncrement().compactAfter().get(0).nonNullFirstRowId()).isZero();
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
