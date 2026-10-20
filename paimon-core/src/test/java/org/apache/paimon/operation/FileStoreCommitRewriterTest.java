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

import org.apache.paimon.AppendOnlyFileStore;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.RenamingSnapshotCommit;
import org.apache.paimon.catalog.SnapshotCommit;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.operation.commit.CommitChanges;
import org.apache.paimon.operation.commit.CommitChangesRewriter;
import org.apache.paimon.operation.commit.ConflictDetection;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.CommitPreCallback;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Commit-attempt isolation and resource ownership for injected index rewriters. */
class FileStoreCommitRewriterTest extends TableTestBase {

    @Test
    void testRetryCreatesAttemptFromLatestSnapshotAndFinalRowIds() throws Exception {
        FileStoreTable table = createTable();
        write(table, GenericRow.of(1));
        ManifestCommittable pending = prepareAppend(table, 10, 20);
        List<Long> snapshots = new ArrayList<>();
        List<Long> firstRowIds = new ArrayList<>();
        List<Path> outputs = new ArrayList<>();
        List<CommitChangesRewriter.Attempt> attempts = new ArrayList<>();
        AtomicInteger publications = new AtomicInteger();
        SnapshotCommit publisher =
                new DelegatingSnapshotCommit(table) {
                    @Override
                    protected boolean commitImpl(
                            String baseUuid,
                            Snapshot snapshot,
                            String branch,
                            List<PartitionStatistics> statistics)
                            throws Exception {
                        if (publications.incrementAndGet() == 1) {
                            write(table, GenericRow.of(2), GenericRow.of(3));
                            assertThat(delegate.commit(baseUuid, snapshot, branch, statistics))
                                    .isFalse();
                            return false;
                        }
                        return delegate.commit(baseUuid, snapshot, branch, statistics);
                    }
                };
        CommitChangesRewriter rewriter =
                new CommitChangesRewriter() {
                    @Override
                    public Attempt createAttempt(Snapshot.CommitKind kind, CommitChanges changes) {
                        assertThat(kind).isEqualTo(Snapshot.CommitKind.APPEND);
                        assertThat(changes.tableFiles).hasSize(1);
                        assertThat(changes.tableFiles.get(0).file().firstRowId()).isNull();
                        assertReadOnly(changes.tableFiles);
                        assertReadOnly(changes.changelogFiles);
                        assertReadOnly(changes.indexFiles);
                        AtomicInteger calls = new AtomicInteger();
                        Attempt attempt =
                                (snapshotId, assigned, indexes, ioManager, register) -> {
                                    assertThat(calls.incrementAndGet()).isEqualTo(1);
                                    snapshots.add(snapshotId);
                                    assertReadOnly(assigned);
                                    assertReadOnly(indexes);
                                    firstRowIds.add(assigned.get(0).file().firstRowId());
                                    return writeIndex(table, assigned.get(0), outputs, register);
                                };
                        attempts.add(attempt);
                        return attempt;
                    }
                };
        try (FileStoreCommit commit =
                newCommit(table, publisher, Collections.emptyList())
                        .withCommitChangesRewriter(rewriter)) {
            commit.commit(pending, false);
        }

        assertThat(publications).hasValue(2);
        assertThat(snapshots).containsExactly(2L, 3L);
        assertThat(firstRowIds).containsExactly(1L, 3L);
        assertThat(attempts).hasSize(2);
        assertThat(attempts.get(0)).isNotSameAs(attempts.get(1));
        assertThat(table.latestSnapshot().get().nextRowId()).isEqualTo(5L);
        assertThat(read(table))
                .extracting(row -> row.getInt(0))
                .containsExactlyInAnyOrder(1, 2, 3, 10, 20);
        List<IndexManifestEntry> indexes = table.store().newIndexFileHandler().scanEntries();
        assertThat(indexes).hasSize(1);
        assertThat(indexes.get(0).indexFile().fileName()).isEqualTo(outputs.get(1).getName());
        assertThat(indexes.get(0).indexFile().globalIndexMeta().rowRange())
                .isEqualTo(new Range(3, 4));
        // A false publication response is uncertain, even when this test's publisher knows it lost.
        for (Path output : outputs) {
            assertThat(table.fileIO().exists(output)).isTrue();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPreparationFailureCleansRegisteredFiles(boolean fatalError) throws Exception {
        FileStoreTable table = createTable();
        ManifestCommittable pending = prepareAppend(table, 10);
        List<Path> outputs = new ArrayList<>();
        Throwable failure =
                fatalError
                        ? new AssertionError("rewrite failed")
                        : new IOException("rewrite failed");
        AtomicInteger publications = new AtomicInteger();
        CommitChangesRewriter rewriter =
                new CommitChangesRewriter() {
                    @Override
                    public Attempt createAttempt(Snapshot.CommitKind kind, CommitChanges changes) {
                        return (snapshotId, assigned, indexes, ioManager, register) -> {
                            writeIndex(table, assigned.get(0), outputs, register);
                            if (fatalError) {
                                throw (AssertionError) failure;
                            }
                            throw (IOException) failure;
                        };
                    }
                };
        try (FileStoreCommit commit =
                newCommit(table, countingPublisher(table, publications), Collections.emptyList())
                        .withCommitChangesRewriter(rewriter)) {
            assertThatThrownBy(() -> commit.commit(pending, false))
                    .hasMessageContaining("preparing snapshot #1")
                    .hasCauseReference(failure);
        }
        assertThat(outputs).hasSize(1);
        assertThat(table.fileIO().exists(outputs.get(0))).isFalse();
        assertThat(publications).hasValue(0);
        assertThat(table.latestSnapshot()).isEmpty();
    }

    @Test
    void testManifestPreparationFailureCleansRegisteredFiles() throws Exception {
        FileStoreTable table = createTable();
        ManifestCommittable pending = prepareAppend(table, 10);
        List<Path> outputs = new ArrayList<>();
        AtomicInteger publications = new AtomicInteger();
        CommitChangesRewriter rewriter =
                new CommitChangesRewriter() {
                    @Override
                    public Attempt createAttempt(Snapshot.CommitKind kind, CommitChanges changes) {
                        return (snapshotId, assigned, indexes, ioManager, register) -> {
                            IndexManifestEntry index =
                                    writeIndex(table, assigned.get(0), outputs, register).get(0);
                            // Fail after the extension returns, while merging the index manifest.
                            return Collections.singletonList(index.toDeleteEntry());
                        };
                    }
                };
        try (FileStoreCommit commit =
                newCommit(table, countingPublisher(table, publications), Collections.emptyList())
                        .withCommitChangesRewriter(rewriter)) {
            assertThatThrownBy(() -> commit.commit(pending, false))
                    .hasMessageContaining("preparing snapshot #1")
                    .hasRootCauseInstanceOf(IllegalStateException.class)
                    .hasStackTraceContaining("which does not exist.");
        }
        assertThat(outputs).hasSize(1);
        assertThat(table.fileIO().exists(outputs.get(0))).isFalse();
        assertThat(publications).hasValue(0);
        assertThat(table.latestSnapshot()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPreCallbackFailureCleansRegisteredFiles(boolean fatalError) throws Exception {
        FileStoreTable table = createTable();
        ManifestCommittable pending = prepareAppend(table, 10);
        List<Path> outputs = new ArrayList<>();
        Throwable failure =
                fatalError
                        ? new AssertionError("validation failed")
                        : new IllegalStateException("validation failed");
        AtomicInteger publications = new AtomicInteger();
        CommitPreCallback callback =
                new CommitPreCallback() {
                    @Override
                    public void call(
                            List<SimpleFileEntry> base,
                            List<ManifestEntry> delta,
                            List<IndexManifestEntry> indexes,
                            Snapshot snapshot) {
                        assertThat(indexes).hasSize(1);
                        if (fatalError) {
                            throw (AssertionError) failure;
                        }
                        throw (IllegalStateException) failure;
                    }

                    @Override
                    public void close() {}
                };
        try (FileStoreCommit commit =
                newCommit(
                                table,
                                countingPublisher(table, publications),
                                Collections.singletonList(callback))
                        .withCommitChangesRewriter(indexRewriter(table, outputs))) {
            assertThatThrownBy(() -> commit.commit(pending, false)).isSameAs(failure);
        }
        assertThat(outputs).hasSize(1);
        assertThat(table.fileIO().exists(outputs.get(0))).isFalse();
        assertThat(publications).hasValue(0);
        assertThat(table.latestSnapshot()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testUncertainPublicationRetainsRegisteredIndex(boolean publicationThrows)
            throws Exception {
        FileStoreTable table = createTable();
        ManifestCommittable pending = prepareAppend(table, 10, 20);
        List<Path> outputs = new ArrayList<>();
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger publications = new AtomicInteger();
        SnapshotCommit publisher =
                new DelegatingSnapshotCommit(table) {
                    @Override
                    protected boolean commitImpl(
                            String baseUuid,
                            Snapshot snapshot,
                            String branch,
                            List<PartitionStatistics> statistics)
                            throws Exception {
                        publications.incrementAndGet();
                        assertThat(delegate.commit(baseUuid, snapshot, branch, statistics))
                                .isTrue();
                        if (publicationThrows) {
                            throw new IOException("publication response lost");
                        }
                        return false;
                    }
                };
        CommitChangesRewriter rewriter =
                new CommitChangesRewriter() {
                    @Override
                    public Attempt createAttempt(Snapshot.CommitKind kind, CommitChanges changes) {
                        attempts.incrementAndGet();
                        return (snapshotId, assigned, indexes, ioManager, register) ->
                                writeIndex(table, assigned.get(0), outputs, register);
                    }
                };
        try (FileStoreCommit commit =
                newCommit(table, publisher, Collections.emptyList())
                        .withCommitChangesRewriter(rewriter)) {
            commit.commit(pending, false);
        }

        assertThat(publications).hasValue(1);
        assertThat(attempts).hasValue(1);
        // Reconciliation must happen before constructing another attempt or rewriting another
        // index.
        assertThat(outputs).hasSize(1);
        assertThat(table.fileIO().readFileUtf8(outputs.get(0))).isEqualTo("index payload");
        assertThat(table.latestSnapshot().get().id()).isEqualTo(1L);
        assertThat(table.latestSnapshot().get().nextRowId()).isEqualTo(2L);
        assertThat(read(table)).extracting(row -> row.getInt(0)).containsExactlyInAnyOrder(10, 20);
        List<IndexManifestEntry> indexes = table.store().newIndexFileHandler().scanEntries();
        assertThat(indexes).hasSize(1);
        assertThat(indexes.get(0).indexFile().fileName()).isEqualTo(outputs.get(0).getName());
        assertThat(indexes.get(0).indexFile().globalIndexMeta().rowRange())
                .isEqualTo(new Range(0, 1));
    }

    private FileStoreTable createTable() throws Exception {
        catalog.createTable(
                identifier(),
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.WRITE_ONLY.key(), "true")
                        .option(CoreOptions.COMMIT_MIN_RETRY_WAIT.key(), "0ms")
                        .option(CoreOptions.COMMIT_MAX_RETRY_WAIT.key(), "0ms")
                        .build(),
                false);
        return getTableDefault();
    }

    private ManifestCommittable prepareAppend(FileStoreTable table, int... values)
            throws Exception {
        ManifestCommittable pending = new ManifestCommittable(17L);
        try (BatchTableWrite write = table.newBatchWriteBuilder().newWrite()) {
            for (int value : values) {
                write.write(GenericRow.of(value));
            }
            write.prepareCommit().forEach(pending::addFileCommittable);
        }
        return pending;
    }

    private CommitChangesRewriter indexRewriter(FileStoreTable table, List<Path> outputs) {
        return new CommitChangesRewriter() {
            @Override
            public Attempt createAttempt(Snapshot.CommitKind kind, CommitChanges changes) {
                return (snapshotId, assigned, indexes, ioManager, register) ->
                        writeIndex(table, assigned.get(0), outputs, register);
            }
        };
    }

    private List<IndexManifestEntry> writeIndex(
            FileStoreTable table,
            ManifestEntry assigned,
            List<Path> outputs,
            Consumer<Path> register)
            throws IOException {
        Path path = table.store().pathFactory().globalIndexFileFactory().newPath();
        register.accept(path);
        outputs.add(path);
        table.fileIO().writeFile(path, "index payload", false);
        IndexFileMeta index =
                new IndexFileMeta(
                        "test-index",
                        path.getName(),
                        13,
                        assigned.file().rowCount(),
                        new GlobalIndexMeta(
                                assigned.file().nonNullFirstRowId(),
                                assigned.file().nonNullRowIdRange().to,
                                0,
                                null,
                                null),
                        null);
        return Collections.singletonList(
                new IndexManifestEntry(
                        FileKind.ADD, assigned.partition(), assigned.bucket(), index));
    }

    private SnapshotCommit countingPublisher(FileStoreTable table, AtomicInteger publications) {
        return new DelegatingSnapshotCommit(table) {
            @Override
            protected boolean commitImpl(
                    String baseUuid,
                    Snapshot snapshot,
                    String branch,
                    List<PartitionStatistics> statistics)
                    throws Exception {
                publications.incrementAndGet();
                return delegate.commit(baseUuid, snapshot, branch, statistics);
            }
        };
    }

    private FileStoreCommitImpl newCommit(
            FileStoreTable table, SnapshotCommit publisher, List<CommitPreCallback> callbacks) {
        AppendOnlyFileStore store = (AppendOnlyFileStore) table.store();
        CoreOptions options = store.options();
        return new FileStoreCommitImpl(
                publisher,
                table.fileIO(),
                new FileSystemSchemaManager(table.fileIO(), table.location()),
                "rewriter-test",
                "rewriter-test-user",
                store.partitionType(),
                options,
                store.pathFactory(),
                store.snapshotManager(),
                store.manifestFileFactory(),
                store.manifestListFactory(),
                store.indexManifestFileFactory(),
                store::newScan,
                store.newStatsFileHandler(),
                store.bucketMode(),
                callbacks,
                Collections.emptyList(),
                scanner ->
                        ConflictDetection.create(
                                "rewriter-test",
                                "rewriter-test-user",
                                store.partitionType(),
                                store.pathFactory(),
                                store.newKeyComparator(),
                                store.bucketMode(),
                                options.deletionVectorsEnabled(),
                                false,
                                false,
                                options.pkClusteringOverride(),
                                store.newIndexFileHandler(),
                                store.snapshotManager(),
                                scanner),
                null);
    }

    private static void assertReadOnly(List<?> entries) {
        assertThatThrownBy(entries::clear).isInstanceOf(UnsupportedOperationException.class);
    }

    private static class DelegatingSnapshotCommit extends SnapshotCommit {

        protected final SnapshotCommit delegate;

        private DelegatingSnapshotCommit(FileStoreTable table) {
            super(Lock.empty());
            delegate = new RenamingSnapshotCommit(table.snapshotManager(), Lock.empty());
        }

        @Override
        protected boolean commitImpl(
                String baseUuid,
                Snapshot snapshot,
                String branch,
                List<PartitionStatistics> statistics)
                throws Exception {
            return delegate.commit(baseUuid, snapshot, branch, statistics);
        }

        @Override
        protected void closeImpl() throws Exception {
            delegate.close();
        }
    }
}
