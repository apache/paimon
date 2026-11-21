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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.TestFileStore;
import org.apache.paimon.TestKeyValueGenerator;
import org.apache.paimon.catalog.RenamingSnapshotCommit;
import org.apache.paimon.catalog.SnapshotCommit;
import org.apache.paimon.data.BlobIngestObservations;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.mergetree.compact.DeduplicateMergeFunction;
import org.apache.paimon.operation.commit.ConflictDetection;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.apache.paimon.utils.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Native Blob ingest observations for uncertain snapshot publication and replay. */
class BlobIngestCommitTest {

    private final TestKeyValueGenerator gen = new TestKeyValueGenerator();
    @TempDir java.nio.file.Path tempDir;

    @AfterEach
    void afterEach() {
        Predicate<Path> pathPredicate = path -> path.toString().contains(tempDir.toString());
        assertThat(TraceableFileIO.openInputStreams(pathPredicate)).isEmpty();
        assertThat(TraceableFileIO.openOutputStreams(pathPredicate)).isEmpty();
    }

    @ParameterizedTest
    @CsvSource({"false, false", "false, true", "true, true"})
    public void testBlobIngestObservesLostPublicationReceiptAndReplay(
            boolean retry, boolean durable) throws Exception {
        TestFileStore store = createStore();
        String commitUser = "writer";
        AtomicReference<ManifestCommittable> preparedRef = new AtomicReference<>();
        store.commitDataImpl(
                Collections.singletonList(gen.nextInsert("20211110", 8, 1L, null, "a")),
                gen::getPartition,
                value -> 0,
                false,
                17L,
                null,
                Collections.emptyList(),
                (commit, committable) -> preparedRef.set(committable));
        ManifestCommittable prepared = checkNotNull(preparedRef.get());
        try (FileStoreCommit baseline = store.newCommit(commitUser, null)) {
            baseline.ignoreEmptyCommit(false);
            baseline.commit(new ManifestCommittable(prepared.identifier() - 1), false);
        }
        long oldSnapshot = store.snapshotManager().latestSnapshot().id();

        SnapshotCommit delegate = new RenamingSnapshotCommit(store.snapshotManager(), Lock.empty());
        SnapshotCommit publisher =
                new SnapshotCommit(delegate) {
                    @Override
                    protected boolean commitImpl(
                            @Nullable String baseSnapshotUuid,
                            Snapshot snapshot,
                            String branch,
                            List<org.apache.paimon.partition.PartitionStatistics> statistics)
                            throws Exception {
                        if (durable) {
                            assertThat(
                                            delegate.commit(
                                                    baseSnapshotUuid, snapshot, branch, statistics))
                                    .isTrue();
                        }
                        throw new IOException("Publication response lost");
                    }
                };
        Options options = Options.fromMap(store.options().toMap());
        options.set(CoreOptions.COMMIT_MAX_RETRIES, retry ? 1 : 0);
        List<String> events = new ArrayList<>();
        try (BlobIngestObservations.Scope observations =
                        BlobIngestObservations.observe(
                                (stage, outcome, value) -> events.add(stage + "/" + outcome));
                FileStoreCommitImpl commit =
                        newCommitWithSnapshotCommit(
                                store, commitUser, publisher, new CoreOptions(options), false)) {
            if (retry) {
                assertThat(commit.commit(prepared, false)).isEqualTo(1);
                assertThat(events)
                        .containsExactly(
                                "commit_publish/attempt",
                                "commit_publish/unknown",
                                "commit_publish/reconciled",
                                "commit/committed",
                                "commit_elapsed_nanos/committed");
            } else {
                assertThatThrownBy(() -> commit.commit(prepared, false))
                        .hasRootCauseMessage("Publication response lost");
                assertThat(events)
                        .containsExactly(
                                "commit_publish/attempt",
                                "commit_publish/unknown",
                                "commit/unknown",
                                "commit_elapsed_nanos/unknown");
            }
        }

        store.snapshotManager().invalidateCache();
        Snapshot snapshot = store.snapshotManager().latestSnapshot();
        assertThat(snapshot.commitUser()).isEqualTo(commitUser);
        assertThat(store.readKvsFromSnapshot(oldSnapshot)).isEmpty();
        if (durable) {
            assertThat(snapshot.id()).isEqualTo(oldSnapshot + 1);
            assertThat(snapshot.commitIdentifier()).isEqualTo(prepared.identifier());
            assertThat(store.readKvsFromSnapshot(snapshot.id()))
                    .extracting(kv -> kv.value().getString(6).toString())
                    .containsExactly("a");
        } else {
            assertThat(snapshot.id()).isEqualTo(oldSnapshot);
            assertThat(snapshot.commitIdentifier()).isEqualTo(prepared.identifier() - 1);
        }

        events.clear();
        try (BlobIngestObservations.Scope observations =
                        BlobIngestObservations.observe(
                                (stage, outcome, value) -> events.add(stage + "/" + outcome));
                FileStoreCommit replay = store.newCommit(commitUser, null)) {
            List<ManifestCommittable> filtered =
                    replay.filterCommitted(Collections.singletonList(prepared));
            if (durable) {
                assertThat(filtered).isEmpty();
                assertThat(events).containsExactly("commit/no_op");
            } else {
                assertThat(filtered).containsExactly(prepared);
                assertThat(events).isEmpty();
            }

            events.clear();
            replay.ignoreEmptyCommit(true);
            assertThat(replay.commit(new ManifestCommittable(prepared.identifier() + 1), false))
                    .isZero();
            assertThat(events).containsExactly("commit/no_op", "commit_elapsed_nanos/no_op");
        }
        assertThat(store.snapshotManager().latestSnapshot().id()).isEqualTo(snapshot.id());
        System.out.printf("IO13_COMMIT|%s|%s|%d|%d%n", retry, durable, oldSnapshot, snapshot.id());
    }

    private TestFileStore createStore() throws Exception {
        String root = TraceableFileIO.SCHEME + "://" + tempDir.toString();
        Path path = new Path(tempDir.toUri());
        Map<String, String> options = Collections.emptyMap();
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(new LocalFileIO(), path),
                        new Schema(
                                TestKeyValueGenerator.DEFAULT_ROW_TYPE.getFields(),
                                TestKeyValueGenerator.DEFAULT_PART_TYPE.getFieldNames(),
                                TestKeyValueGenerator.getPrimaryKeys(
                                        TestKeyValueGenerator.GeneratorMode.MULTI_PARTITIONED),
                                options,
                                null));
        return new TestFileStore.Builder(
                        "avro",
                        root,
                        1,
                        TestKeyValueGenerator.DEFAULT_PART_TYPE,
                        TestKeyValueGenerator.KEY_TYPE,
                        TestKeyValueGenerator.DEFAULT_ROW_TYPE,
                        TestKeyValueGenerator.TestKeyValueFieldsExtractor.EXTRACTOR,
                        DeduplicateMergeFunction.factory(),
                        tableSchema)
                .changelogProducer(CoreOptions.ChangelogProducer.NONE)
                .build();
    }

    private FileStoreCommitImpl newCommitWithSnapshotCommit(
            TestFileStore store,
            String commitUser,
            SnapshotCommit snapshotCommit,
            CoreOptions options,
            boolean dataEvolutionEnabled) {
        String tableName = store.options().path().getName();
        return new FileStoreCommitImpl(
                snapshotCommit,
                store.fileIO(),
                new FileSystemSchemaManager(store.fileIO(), store.options().path()),
                tableName,
                commitUser,
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
                Collections.emptyList(),
                Collections.emptyList(),
                scanner ->
                        ConflictDetection.create(
                                tableName,
                                commitUser,
                                store.partitionType(),
                                store.pathFactory(),
                                store.newKeyComparator(),
                                store.bucketMode(),
                                options.deletionVectorsEnabled(),
                                dataEvolutionEnabled,
                                options.dataEvolutionNestedFieldEnabled(),
                                options.pkClusteringOverride(),
                                store.newIndexFileHandler(),
                                store.snapshotManager(),
                                scanner),
                null);
    }
}
