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

package org.apache.paimon.index.pkvector;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.options.Options;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests ANN maintenance over compact data-file sources. */
class BucketedVectorIndexMaintainerTest {

    @TempDir java.nio.file.Path tempPath;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @AfterEach
    void shutdownExecutor() {
        executor.shutdownNow();
    }

    @Test
    void testRestoreFiltersPayloadsByVectorDefinition() {
        DataField vectorField =
                new DataField(7, "embedding", DataTypes.VECTOR(2, DataTypes.FLOAT()));
        DataFileMeta data = dataFile("data");
        IndexFileMeta vectorPayload = payload("ann", data, 7, "test-vector-ann");
        List<IndexFileMeta> restoredPayloads =
                Arrays.asList(
                        payload("btree", data, 3, "btree"),
                        payload("bitmap", data, 5, "bitmap"),
                        payload("other-ann", data, 8, "test-vector-ann"),
                        vectorPayload);

        BucketedVectorIndexMaintainer maintainer =
                new BucketedVectorIndexMaintainer(
                        7,
                        new PkVectorAnnSegmentFile(LocalFileIO.create(), pathFactory()),
                        vectorField,
                        indexOptions(),
                        "l2",
                        "test-vector-ann",
                        mock(PkVectorDataFileReader.Factory.class),
                        Collections.singletonList(data),
                        restoredPayloads,
                        executor);

        assertThat(maintainer.segments()).containsExactly(vectorPayload);
    }

    @Test
    void testPreparedCommitCanBeAbortedByCoordinator() throws Exception {
        DataField vectorField =
                new DataField(7, "embedding", DataTypes.VECTOR(2, DataTypes.FLOAT()));
        DataFileMeta data = dataFile("data");
        PkVectorDataFileReader.Factory readerFactory = mock(PkVectorDataFileReader.Factory.class);
        PkVectorDataFileReader dataReader = reader(new float[][] {{1, 0}});
        when(readerFactory.create(data)).thenReturn(dataReader);
        BucketedVectorIndexMaintainer maintainer =
                new BucketedVectorIndexMaintainer(
                        7,
                        new PkVectorAnnSegmentFile(LocalFileIO.create(), pathFactory()),
                        vectorField,
                        indexOptions(),
                        "l2",
                        "test-vector-ann",
                        readerFactory,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        executor);

        BucketedVectorIndexMaintainer.VectorIndexCommit commit =
                maintainer.prepareCommit(
                        DataIncrement.emptyIncrement(),
                        new CompactIncrement(
                                Collections.emptyList(),
                                Collections.singletonList(data),
                                Collections.emptyList()),
                        true);
        assertThat(maintainer.segments()).hasSize(1);

        commit.abort(new IllegalStateException("later definition failed"));

        assertThat(maintainer.segments()).isEmpty();
        assertThat(fileCount()).isZero();
    }

    @Test
    void testNonBlockingPrepareCommitPublishesCompletedAnnLater() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        PkVectorAnnSegmentFile annFile = new PkVectorAnnSegmentFile(fileIO, pathFactory());
        DataField vectorField =
                new DataField(7, "embedding", DataTypes.VECTOR(2, DataTypes.FLOAT()));
        DataFileMeta data = dataFile("data");
        PkVectorDataFileReader.Factory readerFactory = mock(PkVectorDataFileReader.Factory.class);
        CountDownLatch buildStarted = new CountDownLatch(1);
        CountDownLatch allowBuild = new CountDownLatch(1);
        AtomicReference<Thread> buildThread = new AtomicReference<>();
        Thread callerThread = Thread.currentThread();
        when(readerFactory.create(data))
                .thenAnswer(
                        ignored -> {
                            buildThread.set(Thread.currentThread());
                            buildStarted.countDown();
                            assertThat(allowBuild.await(30, TimeUnit.SECONDS)).isTrue();
                            return reader(new float[][] {{1, 0}});
                        });
        try {
            BucketedVectorIndexMaintainer maintainer =
                    new BucketedVectorIndexMaintainer(
                            7,
                            annFile,
                            vectorField,
                            indexOptions(),
                            "l2",
                            "test-vector-ann",
                            readerFactory,
                            Collections.emptyList(),
                            Collections.emptyList(),
                            executor);

            BucketedVectorIndexMaintainer.VectorIndexCommit firstCommit =
                    maintainer.prepareCommit(
                            DataIncrement.emptyIncrement(),
                            new CompactIncrement(
                                    Collections.emptyList(),
                                    Collections.singletonList(data),
                                    Collections.emptyList()),
                            false);

            assertThat(firstCommit.compactIncrement()).isEmpty();
            assertThat(buildStarted.await(30, TimeUnit.SECONDS)).isTrue();
            assertThat(buildThread.get()).isNotSameAs(callerThread);
            allowBuild.countDown();

            BucketedVectorIndexMaintainer.VectorIndexCommit secondCommit =
                    maintainer.prepareCommit(
                            DataIncrement.emptyIncrement(),
                            CompactIncrement.emptyIncrement(),
                            true);

            assertThat(secondCommit.appendIncrement()).isPresent();
            assertThat(secondCommit.appendIncrement().get().newIndexFiles()).hasSize(1);
            assertThat(secondCommit.compactIncrement()).isEmpty();
        } finally {
            allowBuild.countDown();
        }
    }

    @Test
    void testDiscardsStaleBuildAndIndexesLatestSources() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        PkVectorAnnSegmentFile annFile = new PkVectorAnnSegmentFile(fileIO, pathFactory());
        DataField vectorField =
                new DataField(7, "embedding", DataTypes.VECTOR(2, DataTypes.FLOAT()));
        DataFileMeta data1 = dataFile("data-1");
        DataFileMeta data2 = dataFile("data-2");
        PkVectorDataFileReader.Factory readerFactory = mock(PkVectorDataFileReader.Factory.class);
        CountDownLatch buildStarted = new CountDownLatch(1);
        CountDownLatch allowBuild = new CountDownLatch(1);
        when(readerFactory.create(data1))
                .thenAnswer(
                        ignored -> {
                            buildStarted.countDown();
                            assertThat(allowBuild.await(30, TimeUnit.SECONDS)).isTrue();
                            return reader(new float[][] {{1, 0}});
                        });
        PkVectorDataFileReader reader2 = reader(new float[][] {{2, 0}});
        when(readerFactory.create(data2)).thenReturn(reader2);

        try {
            BucketedVectorIndexMaintainer maintainer =
                    new BucketedVectorIndexMaintainer(
                            7,
                            annFile,
                            vectorField,
                            indexOptions(),
                            "l2",
                            "test-vector-ann",
                            readerFactory,
                            Collections.emptyList(),
                            Collections.emptyList(),
                            executor);
            maintainer.prepareCommit(
                    DataIncrement.emptyIncrement(),
                    new CompactIncrement(
                            Collections.emptyList(),
                            Collections.singletonList(data1),
                            Collections.emptyList()),
                    false);
            assertThat(buildStarted.await(30, TimeUnit.SECONDS)).isTrue();

            maintainer.prepareCommit(
                    DataIncrement.emptyIncrement(),
                    new CompactIncrement(
                            Collections.singletonList(data1),
                            Collections.singletonList(data2),
                            Collections.emptyList()),
                    false);
            allowBuild.countDown();

            BucketedVectorIndexMaintainer.VectorIndexCommit commit =
                    maintainer.prepareCommit(
                            DataIncrement.emptyIncrement(),
                            CompactIncrement.emptyIncrement(),
                            true);

            assertThat(commit.appendIncrement()).isPresent();
            IndexFileMeta segment = commit.appendIncrement().get().newIndexFiles().get(0);
            assertThat(PrimaryKeyIndexSourceMeta.fromIndexFile(segment).sourceFiles())
                    .extracting(PrimaryKeyIndexSourceFile::fileName)
                    .containsExactly("data-2");
            assertThat(annFile.exists(segment)).isTrue();
        } finally {
            allowBuild.countDown();
        }
    }

    @Test
    void testPartialCompactionKeepsOldAnnAndIndexesNewSource() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        PkVectorAnnSegmentFile annFile = new PkVectorAnnSegmentFile(fileIO, pathFactory());
        DataField vectorField =
                new DataField(7, "embedding", DataTypes.VECTOR(2, DataTypes.FLOAT()));
        Options options = indexOptions();
        options.setString("pk-vector.index.compaction.stale-ratio-threshold", "1.0");
        DataFileMeta data1 = dataFile("data-1");
        DataFileMeta data2 = dataFile("data-2");
        DataFileMeta data3 = dataFile("data-3");
        IndexFileMeta initialAnn =
                annFile.build(
                        Arrays.asList(
                                new PkVectorAnnSegmentFile.Source(
                                        data1, new ArrayReader(new float[][] {{1, 0}})),
                                new PkVectorAnnSegmentFile.Source(
                                        data2, new ArrayReader(new float[][] {{2, 0}}))),
                        vectorField,
                        options,
                        "l2",
                        "test-vector-ann");
        PkVectorDataFileReader.Factory readerFactory = mock(PkVectorDataFileReader.Factory.class);
        PkVectorDataFileReader reader2 = reader(new float[][] {{2, 0}});
        PkVectorDataFileReader reader3 = reader(new float[][] {{3, 0}});
        when(readerFactory.create(data2)).thenReturn(reader2);
        when(readerFactory.create(data3)).thenReturn(reader3);
        BucketedVectorIndexMaintainer maintainer =
                new BucketedVectorIndexMaintainer(
                        7,
                        annFile,
                        vectorField,
                        options,
                        "l2",
                        "test-vector-ann",
                        readerFactory,
                        Arrays.asList(data1, data2),
                        Collections.singletonList(initialAnn),
                        executor);

        BucketedVectorIndexMaintainer.VectorIndexCommit commit =
                maintainer.prepareCommit(
                        DataIncrement.emptyIncrement(),
                        new CompactIncrement(
                                Collections.singletonList(data1),
                                Collections.singletonList(data3),
                                Collections.emptyList()),
                        true);

        BucketedVectorIndexMaintainer.VectorIndexIncrement increment =
                commit.compactIncrement().get();
        assertThat(increment.deletedIndexFiles()).isEmpty();
        assertThat(increment.newIndexFiles()).hasSize(1);
        IndexFileMeta delta = increment.newIndexFiles().get(0);
        assertThat(delta.indexType()).isEqualTo("test-vector-ann");
        assertThat(PrimaryKeyIndexSourceMeta.fromIndexFile(delta).sourceFiles())
                .extracting(PrimaryKeyIndexSourceFile::fileName)
                .containsExactly("data-3");
        assertThat(maintainer.segments()).containsExactly(initialAnn, delta);
    }

    @Test
    void testBuildsAnnForSingleCompactSource() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        PkVectorAnnSegmentFile annFile = new PkVectorAnnSegmentFile(fileIO, pathFactory());
        DataField vectorField =
                new DataField(7, "embedding", DataTypes.VECTOR(2, DataTypes.FLOAT()));
        DataFileMeta data = dataFile("data");
        PkVectorDataFileReader.Factory readerFactory = mock(PkVectorDataFileReader.Factory.class);
        PkVectorDataFileReader dataReader = reader(new float[][] {{1, 0}});
        when(readerFactory.create(data)).thenReturn(dataReader);
        BucketedVectorIndexMaintainer maintainer =
                new BucketedVectorIndexMaintainer(
                        7,
                        annFile,
                        vectorField,
                        indexOptions(),
                        "l2",
                        "test-vector-ann",
                        readerFactory,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        executor);

        BucketedVectorIndexMaintainer.VectorIndexCommit commit =
                maintainer.prepareCommit(
                        DataIncrement.emptyIncrement(),
                        new CompactIncrement(
                                Collections.emptyList(),
                                Collections.singletonList(data),
                                Collections.emptyList()),
                        true);

        assertThat(commit.compactIncrement()).isPresent();
        assertThat(commit.compactIncrement().get().newIndexFiles()).hasSize(1);
    }

    @Test
    void testRebuildsDerivedLevelAndAtomicallyReplacesInputs() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        PkVectorAnnSegmentFile annFile = new PkVectorAnnSegmentFile(fileIO, pathFactory());
        DataField vectorField =
                new DataField(7, "embedding", DataTypes.VECTOR(2, DataTypes.FLOAT()));
        Options options = indexOptions();
        options.setString("pk-vector.index.compaction.level-fanout", "3");
        DataFileMeta data1 = dataFile("data-1");
        DataFileMeta data2 = dataFile("data-2");
        DataFileMeta data3 = dataFile("data-3");
        IndexFileMeta ann1 =
                annFile.build(
                        Collections.singletonList(
                                new PkVectorAnnSegmentFile.Source(
                                        data1, new ArrayReader(new float[][] {{1, 0}}))),
                        vectorField,
                        options,
                        "l2",
                        "test-vector-ann");
        IndexFileMeta ann2 =
                annFile.build(
                        Collections.singletonList(
                                new PkVectorAnnSegmentFile.Source(
                                        data2, new ArrayReader(new float[][] {{2, 0}}))),
                        vectorField,
                        options,
                        "l2",
                        "test-vector-ann");
        IndexFileMeta ann3 =
                annFile.build(
                        Collections.singletonList(
                                new PkVectorAnnSegmentFile.Source(
                                        data3, new ArrayReader(new float[][] {{3, 0}}))),
                        vectorField,
                        options,
                        "l2",
                        "test-vector-ann");
        PkVectorDataFileReader.Factory readerFactory = mock(PkVectorDataFileReader.Factory.class);
        PkVectorDataFileReader reader1 = reader(new float[][] {{1, 0}});
        PkVectorDataFileReader reader2 = reader(new float[][] {{2, 0}});
        PkVectorDataFileReader reader3 = reader(new float[][] {{3, 0}});
        when(readerFactory.create(data1)).thenReturn(reader1);
        when(readerFactory.create(data2)).thenReturn(reader2);
        when(readerFactory.create(data3)).thenReturn(reader3);
        BucketedVectorIndexMaintainer maintainer =
                new BucketedVectorIndexMaintainer(
                        7,
                        annFile,
                        vectorField,
                        options,
                        "l2",
                        "test-vector-ann",
                        readerFactory,
                        Arrays.asList(data1, data2, data3),
                        Arrays.asList(ann3, ann1, ann2),
                        executor);

        BucketedVectorIndexMaintainer.VectorIndexCommit commit =
                maintainer.prepareCommit(
                        DataIncrement.emptyIncrement(), CompactIncrement.emptyIncrement(), true);

        BucketedVectorIndexMaintainer.VectorIndexIncrement increment =
                commit.appendIncrement().get();
        assertThat(increment.deletedIndexFiles()).containsExactlyInAnyOrder(ann1, ann2, ann3);
        assertThat(increment.newIndexFiles()).hasSize(1);
        assertThat(
                        PrimaryKeyIndexSourceMeta.fromIndexFile(increment.newIndexFiles().get(0))
                                .sourceFiles())
                .extracting(PrimaryKeyIndexSourceFile::fileName)
                .containsExactly("data-1", "data-2", "data-3");
    }

    @Test
    void testPrepareCommitRollsBackMultipleRebuildsOnFailure() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        PkVectorAnnSegmentFile annFile = new PkVectorAnnSegmentFile(fileIO, pathFactory());
        DataField vectorField =
                new DataField(7, "embedding", DataTypes.VECTOR(2, DataTypes.FLOAT()));
        Options options = indexOptions();
        options.setString("pk-vector.index.compaction.level-fanout", "3");
        List<DataFileMeta> dataFiles = new ArrayList<>();
        List<IndexFileMeta> initialSegments = new ArrayList<>();
        for (int i = 1; i <= 6; i++) {
            DataFileMeta data = dataFile("data-" + i);
            dataFiles.add(data);
            initialSegments.add(
                    annFile.build(
                            Collections.singletonList(
                                    new PkVectorAnnSegmentFile.Source(
                                            data, new ArrayReader(new float[][] {{(float) i, 0}}))),
                            vectorField,
                            options,
                            "l2",
                            "test-vector-ann"));
        }

        AtomicInteger readerCount = new AtomicInteger();
        PkVectorDataFileReader.Factory readerFactory = mock(PkVectorDataFileReader.Factory.class);
        when(readerFactory.create(org.mockito.ArgumentMatchers.any(DataFileMeta.class)))
                .thenAnswer(
                        ignored -> {
                            if (readerCount.incrementAndGet() == 4) {
                                throw new IOException("Expected second ANN rebuild failure.");
                            }
                            return reader(new float[][] {{1, 0}});
                        });
        BucketedVectorIndexMaintainer maintainer =
                new BucketedVectorIndexMaintainer(
                        7,
                        annFile,
                        vectorField,
                        options,
                        "l2",
                        "test-vector-ann",
                        readerFactory,
                        dataFiles,
                        initialSegments,
                        executor);

        assertThatThrownBy(
                        () ->
                                maintainer.prepareCommit(
                                        DataIncrement.emptyIncrement(),
                                        CompactIncrement.emptyIncrement(),
                                        true))
                .isInstanceOf(UncheckedIOException.class);
        assertThat(maintainer.segments()).containsExactlyInAnyOrderElementsOf(initialSegments);
        assertThat(fileCount()).isEqualTo(6);

        BucketedVectorIndexMaintainer.VectorIndexCommit retry =
                maintainer.prepareCommit(
                        DataIncrement.emptyIncrement(), CompactIncrement.emptyIncrement(), true);

        BucketedVectorIndexMaintainer.VectorIndexIncrement increment =
                retry.appendIncrement().get();
        assertThat(increment.deletedIndexFiles())
                .containsExactlyInAnyOrderElementsOf(initialSegments);
        assertThat(increment.newIndexFiles()).hasSize(2);
    }

    @Test
    void testInterruptedPrepareCommitCleansPendingBuildOutput() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        PkVectorAnnSegmentFile annFile = new PkVectorAnnSegmentFile(fileIO, pathFactory());
        DataField vectorField =
                new DataField(7, "embedding", DataTypes.VECTOR(2, DataTypes.FLOAT()));
        DataFileMeta data = dataFile("data");
        CountDownLatch buildStarted = new CountDownLatch(1);
        CountDownLatch allowBuild = new CountDownLatch(1);
        PkVectorDataFileReader.Factory readerFactory = mock(PkVectorDataFileReader.Factory.class);
        when(readerFactory.create(data))
                .thenAnswer(
                        ignored -> {
                            buildStarted.countDown();
                            boolean released = false;
                            while (!released) {
                                try {
                                    released = allowBuild.await(30, TimeUnit.SECONDS);
                                } catch (InterruptedException e) {
                                    // Simulate a native ANN build which does not stop immediately.
                                }
                            }
                            return reader(new float[][] {{1, 0}});
                        });
        BucketedVectorIndexMaintainer maintainer =
                new BucketedVectorIndexMaintainer(
                        7,
                        annFile,
                        vectorField,
                        indexOptions(),
                        "l2",
                        "test-vector-ann",
                        readerFactory,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        executor);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread caller =
                new Thread(
                        () -> {
                            try {
                                maintainer.prepareCommit(
                                        DataIncrement.emptyIncrement(),
                                        new CompactIncrement(
                                                Collections.emptyList(),
                                                Collections.singletonList(data),
                                                Collections.emptyList()),
                                        true);
                            } catch (Throwable t) {
                                failure.set(t);
                            }
                        });

        try {
            caller.start();
            assertThat(buildStarted.await(30, TimeUnit.SECONDS)).isTrue();
            caller.interrupt();
            caller.join(TimeUnit.SECONDS.toMillis(30));
            assertThat(caller.isAlive()).isFalse();
            assertThat(failure.get()).isInstanceOf(InterruptedException.class);
            maintainer.close();
        } finally {
            allowBuild.countDown();
        }
        executor.submit(() -> {}).get(30, TimeUnit.SECONDS);

        assertThat(fileCount()).isZero();
        assertThat(maintainer.segments()).isEmpty();
        assertThat(maintainer.buildNotCompleted()).isFalse();
    }

    @Test
    void testPrepareCommitRollsBackSourceFilesOnFailure() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        PkVectorAnnSegmentFile annFile = new PkVectorAnnSegmentFile(fileIO, pathFactory());
        DataField vectorField =
                new DataField(7, "embedding", DataTypes.VECTOR(2, DataTypes.FLOAT()));
        Options options = indexOptions();
        DataFileMeta oldData = dataFile("old-data");
        DataFileMeta newData = dataFile("new-data");
        IndexFileMeta oldAnn =
                annFile.build(
                        Collections.singletonList(
                                new PkVectorAnnSegmentFile.Source(
                                        oldData, new ArrayReader(new float[][] {{1, 0}}))),
                        vectorField,
                        options,
                        "l2",
                        "test-vector-ann");
        AtomicInteger readerCount = new AtomicInteger();
        PkVectorDataFileReader.Factory readerFactory = mock(PkVectorDataFileReader.Factory.class);
        when(readerFactory.create(newData))
                .thenAnswer(
                        ignored -> {
                            if (readerCount.incrementAndGet() == 1) {
                                throw new IOException("Expected ANN build failure.");
                            }
                            return reader(new float[][] {{2, 0}});
                        });
        BucketedVectorIndexMaintainer maintainer =
                new BucketedVectorIndexMaintainer(
                        7,
                        annFile,
                        vectorField,
                        options,
                        "l2",
                        "test-vector-ann",
                        readerFactory,
                        Collections.singletonList(oldData),
                        Collections.singletonList(oldAnn),
                        executor);

        assertThatThrownBy(
                        () ->
                                maintainer.prepareCommit(
                                        DataIncrement.emptyIncrement(),
                                        new CompactIncrement(
                                                Collections.singletonList(oldData),
                                                Collections.singletonList(newData),
                                                Collections.emptyList()),
                                        true))
                .isInstanceOf(UncheckedIOException.class);

        BucketedVectorIndexMaintainer.VectorIndexCommit retry =
                maintainer.prepareCommit(
                        DataIncrement.emptyIncrement(), CompactIncrement.emptyIncrement(), true);

        assertThat(retry.appendIncrement()).isEmpty();
        assertThat(retry.compactIncrement()).isEmpty();
        assertThat(maintainer.segments()).containsExactly(oldAnn);
    }

    @Test
    void testRejectedBuildSubmissionDoesNotPoisonMaintainer() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        PkVectorAnnSegmentFile annFile = new PkVectorAnnSegmentFile(fileIO, pathFactory());
        DataField vectorField =
                new DataField(7, "embedding", DataTypes.VECTOR(2, DataTypes.FLOAT()));
        DataFileMeta data = dataFile("data");
        PkVectorDataFileReader.Factory readerFactory = mock(PkVectorDataFileReader.Factory.class);
        PkVectorDataFileReader dataReader = reader(new float[][] {{1, 0}});
        when(readerFactory.create(data)).thenReturn(dataReader);
        ExecutorService rejectingExecutor = Executors.newSingleThreadExecutor();
        rejectingExecutor.shutdownNow();
        BucketedVectorIndexMaintainer maintainer =
                new BucketedVectorIndexMaintainer(
                        7,
                        annFile,
                        vectorField,
                        indexOptions(),
                        "l2",
                        "test-vector-ann",
                        readerFactory,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        rejectingExecutor);
        CompactIncrement increment =
                new CompactIncrement(
                        Collections.emptyList(),
                        Collections.singletonList(data),
                        Collections.emptyList());

        Throwable failure =
                catchThrowable(
                        () ->
                                maintainer.prepareCommit(
                                        DataIncrement.emptyIncrement(), increment, true));

        assertThat(failure).isInstanceOf(RejectedExecutionException.class);
        assertThat(failure.getSuppressed()).isEmpty();
        assertThat(maintainer.buildNotCompleted()).isFalse();
        maintainer.withExecutor(executor);

        BucketedVectorIndexMaintainer.VectorIndexCommit retry =
                maintainer.prepareCommit(DataIncrement.emptyIncrement(), increment, true);
        assertThat(retry.compactIncrement()).isPresent();
        assertThat(retry.compactIncrement().get().newIndexFiles()).hasSize(1);
    }

    private static Options indexOptions() {
        Options options = new Options();
        options.setString("test.vector.dimension", "2");
        options.setString("test.vector.metric", "l2");
        return options;
    }

    private static PkVectorDataFileReader reader(float[][] vectors) throws IOException {
        PkVectorDataFileReader reader = mock(PkVectorDataFileReader.class);
        when(reader.dimension()).thenReturn(2);
        when(reader.rowCount()).thenReturn((long) vectors.length);
        final int[] position = {0};
        when(reader.readNextVector(org.mockito.ArgumentMatchers.any(float[].class)))
                .thenAnswer(
                        invocation -> {
                            float[] vector = vectors[position[0]++];
                            System.arraycopy(
                                    vector, 0, invocation.getArgument(0), 0, vector.length);
                            return true;
                        });
        return reader;
    }

    private static DataFileMeta dataFile(String fileName) {
        return DataFileMeta.forAppend(
                        fileName,
                        100,
                        1,
                        SimpleStats.EMPTY_STATS,
                        0,
                        0,
                        1,
                        Collections.emptyList(),
                        null,
                        FileSource.COMPACT,
                        null,
                        null,
                        null,
                        null)
                .upgrade(1);
    }

    private static IndexFileMeta payload(
            String fileName, DataFileMeta source, int fieldId, String indexType) {
        PrimaryKeyIndexSourceFile sourceFile =
                new PrimaryKeyIndexSourceFile(source.fileName(), source.rowCount());
        return new IndexFileMeta(
                indexType,
                fileName,
                1,
                source.rowCount(),
                new GlobalIndexMeta(
                        0,
                        source.rowCount() - 1,
                        fieldId,
                        null,
                        new byte[] {1},
                        new PrimaryKeyIndexSourceMeta(sourceFile).serialize()),
                null);
    }

    private IndexPathFactory pathFactory() {
        Path directory = new Path(tempPath.toUri());
        return new IndexPathFactory() {
            @Override
            public Path toPath(String fileName) {
                return new Path(directory, fileName);
            }

            @Override
            public Path newPath() {
                return new Path(directory, UUID.randomUUID().toString());
            }

            @Override
            public boolean isExternalPath() {
                return false;
            }
        };
    }

    private long fileCount() throws IOException {
        try (java.util.stream.Stream<java.nio.file.Path> files =
                java.nio.file.Files.list(tempPath)) {
            return files.count();
        }
    }

    private static class ArrayReader implements PkVectorReader {

        private final float[][] vectors;
        private int position;

        private ArrayReader(float[][] vectors) {
            this.vectors = vectors;
        }

        @Override
        public int dimension() {
            return 2;
        }

        @Override
        public long rowCount() {
            return vectors.length;
        }

        @Override
        public boolean readNextVector(float[] reuse) {
            float[] vector = vectors[position++];
            System.arraycopy(vector, 0, reuse, 0, vector.length);
            return true;
        }

        @Override
        public void close() throws IOException {}
    }
}
