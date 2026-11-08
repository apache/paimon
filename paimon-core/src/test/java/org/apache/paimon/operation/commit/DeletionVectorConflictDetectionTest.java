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
import org.apache.paimon.Snapshot;
import org.apache.paimon.TestAppendFileStore;
import org.apache.paimon.catalog.RenamingSnapshotCommit;
import org.apache.paimon.catalog.SnapshotCommit;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.operation.FileStoreCommitImpl;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/** Deterministic DV conflicts across snapshot publication and retry. */
class DeletionVectorConflictDetectionTest {

    @TempDir java.nio.file.Path tempDir;

    @AfterEach
    void checkClosedStreams() {
        assertThat(TraceableFileIO.openInputStreams(p -> p.toString().contains(tempDir.toString())))
                .isEmpty();
        assertThat(
                        TraceableFileIO.openOutputStreams(
                                p -> p.toString().contains(tempDir.toString())))
                .isEmpty();
    }

    @ParameterizedTest
    @CsvSource({
        "different-file,false,false,false",
        "different-file,true,false,true",
        "different-bucket,true,false,false",
        "different-partition,true,false,false",
        "same-file,false,false,true",
        "same-file,true,false,true",
        "delete-file,false,false,true",
        "delete-before-dv,false,false,true",
        "different-file,false,true,false",
        "different-file,true,true,true",
        "different-bucket,true,true,false",
        "different-partition,true,true,false",
        "same-file,false,true,true",
        "same-file,true,true,true",
        "delete-file,false,true,true",
        "delete-before-dv,false,true,true"
    })
    void testConcurrentDvCommit(
            String competingChange, boolean existingDv, boolean dataEvolution, boolean conflict)
            throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "-1");
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        options.put(CoreOptions.COMMIT_MIN_RETRY_WAIT.key(), "1 ms");
        options.put(CoreOptions.COMMIT_MAX_RETRY_WAIT.key(), "1 ms");
        if (dataEvolution) {
            options.put(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
            options.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        }
        TestAppendFileStore store = TestAppendFileStore.createAppendStore(tempDir, options);
        BinaryRow partition = partition(0);
        BinaryRow otherPartition = partition(1);
        store.commit(
                writeDataFiles(store, partition, 0, Arrays.asList("left", "right")),
                writeDataFiles(store, partition, 1, Collections.singletonList("other-bucket")),
                writeDataFiles(
                        store, otherPartition, 0, Collections.singletonList("other-partition")));
        if (existingDv) {
            Map<String, List<Integer>> initial = new HashMap<>();
            initial.put("left", Collections.singletonList(0));
            initial.put("right", Collections.singletonList(0));
            store.commit(
                    store.writeDVIndexFiles(partition, 0, initial),
                    store.writeDVIndexFiles(
                            partition,
                            1,
                            Collections.singletonMap("other-bucket", Collections.singletonList(0))),
                    store.writeDVIndexFiles(
                            otherPartition,
                            0,
                            Collections.singletonMap(
                                    "other-partition", Collections.singletonList(0))));
        }
        long baseId = store.snapshotManager().latestSnapshotId();
        BinaryRow competingPartition =
                competingChange.equals("different-partition") ? otherPartition : partition;
        int competingBucket = competingChange.equals("different-bucket") ? 1 : 0;
        String competingFile;
        switch (competingChange) {
            case "different-partition":
                competingFile = "other-partition";
                break;
            case "different-bucket":
                competingFile = "other-bucket";
                break;
            case "different-file":
                competingFile = "right";
                break;
            default:
                competingFile = "left";
        }
        // Both writers prepare against the same snapshot, before either publishes its DV update.
        ManifestCommittable first =
                competingChange.equals("delete-before-dv")
                        ? deleteFile(store, partition, "left")
                        : updateDv(store, partition, 0, "left", 1);
        ManifestCommittable second =
                competingChange.equals("delete-file")
                        ? deleteFile(store, partition, "left")
                        : updateDv(store, competingPartition, competingBucket, competingFile, 2);
        IndexFileHandler conflictIndexes = spy(store.newIndexFileHandler());
        PausingSnapshotCommit publisher = new PausingSnapshotCommit(store.snapshotManager());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Integer> result =
                executor.submit(
                        () -> {
                            try (FileStoreCommitImpl commit =
                                    newCommit(store, publisher, conflictIndexes, dataEvolution)) {
                                return commit.commit(first, false);
                            } finally {
                                publisher.prepared.countDown();
                            }
                        });
        try {
            assertThat(publisher.prepared.await(30, TimeUnit.SECONDS)).isTrue();
            if (result.isDone()) {
                result.get(30, TimeUnit.SECONDS);
            }
            try (FileStoreCommitImpl competingCommit = store.newCommit("competitor", null)) {
                assertThat(competingCommit.commit(second, false)).isEqualTo(1);
            }
            publisher.resume.countDown();
            if (conflict) {
                assertThatThrownBy(() -> result.get(30, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class)
                        .hasStackTraceContaining("File deletion conflicts detected");
                assertThat(publisher.attempts).hasValue(1);
            } else {
                assertThat(result.get(30, TimeUnit.SECONDS)).isEqualTo(1);
                assertThat(publisher.attempts).hasValue(2);
            }
            assertThat(store.snapshotManager().latestSnapshotId())
                    .isEqualTo(baseId + (conflict ? 1 : 2));
            assertThat(store.newScan().plan().files())
                    .extracting(entry -> entry.file().fileName())
                    .contains("right", "other-bucket", "other-partition");
            if (competingChange.equals("delete-file")) {
                assertThat(store.newScan().plan().files())
                        .extracting(entry -> entry.file().fileName())
                        .doesNotContain("left");
            } else {
                assertDv(
                        store,
                        partition,
                        0,
                        "left",
                        existingDv,
                        !conflict,
                        competingFile.equals("left"));
                assertDv(
                        store,
                        competingPartition,
                        competingBucket,
                        competingFile,
                        existingDv,
                        !conflict && competingFile.equals("left"),
                        true);
            }
            if (existingDv) {
                assertDv(
                        store,
                        partition,
                        1,
                        "other-bucket",
                        true,
                        false,
                        competingChange.equals("different-bucket"));
                assertDv(
                        store,
                        otherPartition,
                        0,
                        "other-partition",
                        true,
                        false,
                        competingChange.equals("different-partition"));
                // With a shared input DV index, even different data files must reject a stale
                // replacement; otherwise one writer can restore rows deleted by its competitor.
                ArgumentCaptor<Snapshot> snapshots = ArgumentCaptor.forClass(Snapshot.class);
                verify(conflictIndexes, atLeastOnce())
                        .scanBucketEntries(
                                snapshots.capture(),
                                eq(DELETION_VECTORS_INDEX),
                                eq(Collections.singleton(Pair.of(partition, 0))));
                assertThat(snapshots.getAllValues())
                        .extracting(Snapshot::id)
                        .contains(baseId, baseId + 1);
            }
            verify(conflictIndexes, never()).readManifest(anyString());
        } finally {
            publisher.resume.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    private CommitMessageImpl writeDataFiles(
            TestAppendFileStore store, BinaryRow partition, int bucket, List<String> names)
            throws IOException {
        CommitMessageImpl message = store.writeDataFiles(partition, bucket, names);
        List<DataFileMeta> files = new ArrayList<>();
        for (DataFileMeta file : message.newFilesIncrement().newFiles()) {
            files.add(file.assignFileSource(FileSource.APPEND));
        }
        return new CommitMessageImpl(
                partition,
                bucket,
                -1,
                new DataIncrement(files, Collections.emptyList(), Collections.emptyList()),
                CompactIncrement.emptyIncrement());
    }

    private ManifestCommittable updateDv(
            TestAppendFileStore store, BinaryRow partition, int bucket, String file, int position) {
        ManifestCommittable changes = new ManifestCommittable(100);
        List<IndexFileMeta> inputs = store.scanDVIndexFiles(partition, bucket);
        changes.addFileCommittable(store.removeIndexFiles(partition, bucket, inputs));
        changes.addFileCommittable(
                store.writeDVIndexFiles(
                        partition,
                        bucket,
                        Collections.singletonMap(file, Collections.singletonList(position))));
        return changes;
    }

    private ManifestCommittable deleteFile(
            TestAppendFileStore store, BinaryRow partition, String file) {
        DataFileMeta input =
                store.newScan().plan().files().stream()
                        .filter(
                                entry ->
                                        entry.partition().equals(partition)
                                                && entry.file().fileName().equals(file))
                        .findFirst()
                        .get()
                        .file();
        ManifestCommittable changes = new ManifestCommittable(100);
        changes.addFileCommittable(
                new CommitMessageImpl(
                        partition,
                        0,
                        -1,
                        new DataIncrement(
                                Collections.emptyList(),
                                Collections.singletonList(input),
                                Collections.emptyList()),
                        CompactIncrement.emptyIncrement()));
        return changes;
    }

    private void assertDv(
            TestAppendFileStore store,
            BinaryRow partition,
            int bucket,
            String file,
            boolean initial,
            boolean first,
            boolean second) {
        DeletionVector dv =
                store.createOrRestoreDVMaintainer(partition, bucket).deletionVectors().get(file);
        assertThat(dv).isNotNull();
        assertThat(dv.isDeleted(0)).isEqualTo(initial);
        assertThat(dv.isDeleted(1)).isEqualTo(first);
        assertThat(dv.isDeleted(2)).isEqualTo(second);
        assertThat(dv.isDeleted(3)).isFalse();
    }

    private BinaryRow partition(int hour) {
        BinaryRow partition = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(partition);
        writer.writeString(0, BinaryString.fromString("20261108"));
        writer.writeInt(1, hour);
        writer.complete();
        return partition;
    }

    private FileStoreCommitImpl newCommit(
            TestAppendFileStore store,
            SnapshotCommit publisher,
            IndexFileHandler indexes,
            boolean dataEvolution) {
        String tableName = store.options().path().getName();
        return new FileStoreCommitImpl(
                publisher,
                store.fileIO(),
                new FileSystemSchemaManager(store.fileIO(), store.options().path()),
                tableName,
                "paused-writer",
                store.partitionType(),
                store.options(),
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
                                "paused-writer",
                                store.partitionType(),
                                store.pathFactory(),
                                null,
                                store.bucketMode(),
                                true,
                                dataEvolution,
                                false,
                                false,
                                indexes,
                                store.snapshotManager(),
                                scanner),
                null);
    }

    private static class PausingSnapshotCommit extends RenamingSnapshotCommit {
        private final CountDownLatch prepared = new CountDownLatch(1);
        private final CountDownLatch resume = new CountDownLatch(1);
        private final AtomicInteger attempts = new AtomicInteger();

        private PausingSnapshotCommit(SnapshotManager snapshots) {
            super(snapshots, Lock.empty());
        }

        @Override
        protected boolean commitImpl(
                String baseSnapshotUuid,
                Snapshot snapshot,
                String branch,
                List<PartitionStatistics> statistics)
                throws Exception {
            if (attempts.incrementAndGet() == 1) {
                prepared.countDown();
                assertThat(resume.await(30, TimeUnit.SECONDS)).isTrue();
            }
            return super.commitImpl(baseSnapshotUuid, snapshot, branch, statistics);
        }
    }
}
