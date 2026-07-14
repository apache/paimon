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

package org.apache.paimon.index.pk;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.index.pksorted.BucketedSortedIndexMaintainer;
import org.apache.paimon.index.pksorted.PkSortedIndexFile;
import org.apache.paimon.index.pkvector.BucketedVectorIndexMaintainer;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link BucketedPrimaryKeyIndexMaintainer}. */
class BucketedPrimaryKeyIndexMaintainerTest {

    @TempDir java.nio.file.Path tempPath;
    private final ExecutorService buildExecutor = Executors.newFixedThreadPool(2);

    @AfterEach
    void shutdownExecutor() {
        buildExecutor.shutdownNow();
    }

    @Test
    void testDelegatesVectorLifecycle() {
        BucketedVectorIndexMaintainer vector = mock(BucketedVectorIndexMaintainer.class);
        ExecutorService executor = mock(ExecutorService.class);
        when(vector.buildNotCompleted()).thenReturn(true);
        BucketedPrimaryKeyIndexMaintainer maintainer =
                BucketedPrimaryKeyIndexMaintainer.ofVector(vector);

        assertThat(maintainer.buildNotCompleted()).isTrue();
        maintainer.withExecutor(executor);
        maintainer.close();

        verify(vector).withExecutor(executor);
        verify(vector).close();
    }

    @Test
    void testScalarFailureDoesNotSuppressOtherDefinitionsOrVector() throws Exception {
        DataFileMeta source = dataFile("data-1", 3);
        IndexFileMeta vectorPayload =
                new IndexFileMeta("vector", "vector", 1, 3, (GlobalIndexMeta) null, null);
        BucketedVectorIndexMaintainer vector = mock(BucketedVectorIndexMaintainer.class);
        BucketedVectorIndexMaintainer.VectorIndexCommit vectorCommit =
                mock(BucketedVectorIndexMaintainer.VectorIndexCommit.class);
        BucketedVectorIndexMaintainer.VectorIndexIncrement vectorIncrement =
                mock(BucketedVectorIndexMaintainer.VectorIndexIncrement.class);
        when(vector.prepareCommit(any(), any(), eq(true))).thenReturn(vectorCommit);
        when(vectorCommit.appendIncrement()).thenReturn(Optional.empty());
        when(vectorCommit.compactIncrement()).thenReturn(Optional.of(vectorIncrement));
        when(vectorIncrement.newIndexFiles()).thenReturn(Collections.singletonList(vectorPayload));
        when(vectorIncrement.deletedIndexFiles()).thenReturn(Collections.emptyList());

        List<String> buildOrder = Collections.synchronizedList(new ArrayList<>());
        BucketedSortedIndexMaintainer btree =
                sortedMaintainer(
                        7,
                        "btree",
                        source,
                        dataFile -> {
                            buildOrder.add("btree");
                            throw new IllegalStateException("expected BTree failure");
                        });
        IndexFileMeta bitmapPayload = payload("bitmap", source, 3, 8, "bitmap");
        BucketedSortedIndexMaintainer bitmap =
                sortedMaintainer(
                        8,
                        "bitmap",
                        source,
                        dataFile -> {
                            buildOrder.add("bitmap");
                            return Collections.singletonList(bitmapPayload);
                        });
        BucketedPrimaryKeyIndexMaintainer maintainer =
                BucketedPrimaryKeyIndexMaintainer.of(vector, Arrays.asList(bitmap, btree));
        CompactIncrement compactIncrement = compactAfter(source);

        maintainer.prepareCommit(DataIncrement.emptyIncrement(), compactIncrement, true);

        assertThat(buildOrder).containsExactly("btree", "btree", "btree", "bitmap");
        assertThat(compactIncrement.newIndexFiles()).containsExactly(vectorPayload, bitmapPayload);
        assertThat(compactIncrement.deletedIndexFiles()).isEmpty();
        assertThat(maintainer.buildNotCompleted()).isFalse();
    }

    @Test
    void testScalarDefinitionsNeverBuildConcurrently() throws Exception {
        DataFileMeta source = dataFile("data-1", 3);
        AtomicInteger activeBuilds = new AtomicInteger();
        AtomicInteger peakBuilds = new AtomicInteger();
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);
        BucketedSortedIndexMaintainer first =
                sortedMaintainer(
                        7,
                        "btree",
                        source,
                        dataFile -> {
                            enterBuild(activeBuilds, peakBuilds);
                            firstStarted.countDown();
                            releaseFirst.await();
                            activeBuilds.decrementAndGet();
                            return Collections.singletonList(
                                    payload("btree", source, 3, 7, "btree"));
                        });
        BucketedSortedIndexMaintainer second =
                sortedMaintainer(
                        8,
                        "bitmap",
                        source,
                        dataFile -> {
                            enterBuild(activeBuilds, peakBuilds);
                            secondStarted.countDown();
                            activeBuilds.decrementAndGet();
                            return Collections.singletonList(
                                    payload("bitmap", source, 3, 8, "bitmap"));
                        });
        BucketedPrimaryKeyIndexMaintainer maintainer =
                BucketedPrimaryKeyIndexMaintainer.ofSorted(Arrays.asList(second, first));

        maintainer.prepareCommit(DataIncrement.emptyIncrement(), compactAfter(source), false);
        assertThat(firstStarted.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(secondStarted.getCount()).isEqualTo(1);
        assertThat(maintainer.buildNotCompleted()).isTrue();

        releaseFirst.countDown();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (secondStarted.getCount() > 0 && System.nanoTime() < deadline) {
            maintainer.prepareCommit(
                    DataIncrement.emptyIncrement(), CompactIncrement.emptyIncrement(), false);
            secondStarted.await(10, TimeUnit.MILLISECONDS);
        }
        assertThat(secondStarted.getCount()).isZero();
        maintainer.prepareCommit(
                DataIncrement.emptyIncrement(), CompactIncrement.emptyIncrement(), true);

        assertThat(peakBuilds).hasValue(1);
        assertThat(maintainer.buildNotCompleted()).isFalse();
    }

    @Test
    void testBlockingPrepareRollsBackAllDefinitionsAfterInterruption() throws Exception {
        DataFileMeta source = dataFile("data-1", 3);
        AtomicInteger firstBuilds = new AtomicInteger();
        AtomicInteger secondBuilds = new AtomicInteger();
        CountDownLatch secondStarted = new CountDownLatch(1);
        CountDownLatch blockSecond = new CountDownLatch(1);
        BucketedSortedIndexMaintainer first =
                sortedMaintainer(
                        7,
                        "btree",
                        source,
                        dataFile -> {
                            int attempt = firstBuilds.incrementAndGet();
                            return Collections.singletonList(
                                    payload("btree-" + attempt, source, 3, 7, "btree"));
                        });
        BucketedSortedIndexMaintainer second =
                sortedMaintainer(
                        8,
                        "bitmap",
                        source,
                        dataFile -> {
                            int attempt = secondBuilds.incrementAndGet();
                            if (attempt == 1) {
                                secondStarted.countDown();
                                blockSecond.await();
                            }
                            return Collections.singletonList(
                                    payload("bitmap-" + attempt, source, 3, 8, "bitmap"));
                        });
        BucketedPrimaryKeyIndexMaintainer maintainer =
                BucketedPrimaryKeyIndexMaintainer.ofSorted(Arrays.asList(second, first));
        CompactIncrement failedIncrement = compactAfter(source);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread commitThread =
                new Thread(
                        () -> {
                            try {
                                maintainer.prepareCommit(
                                        DataIncrement.emptyIncrement(), failedIncrement, true);
                            } catch (Throwable t) {
                                failure.set(t);
                            }
                        });

        commitThread.start();
        assertThat(secondStarted.await(5, TimeUnit.SECONDS)).isTrue();
        commitThread.interrupt();
        commitThread.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(commitThread.isAlive()).isFalse();
        assertThat(failure.get()).isInstanceOf(InterruptedException.class);
        assertThat(failedIncrement.newIndexFiles()).isEmpty();
        assertThat(failedIncrement.deletedIndexFiles()).isEmpty();
        assertThat(first.state().groups()).isEmpty();
        assertThat(second.state().groups()).isEmpty();

        CompactIncrement retryIncrement = compactAfter(source);
        maintainer.prepareCommit(DataIncrement.emptyIncrement(), retryIncrement, true);

        assertThat(firstBuilds).hasValue(2);
        assertThat(secondBuilds).hasValue(2);
        assertThat(retryIncrement.newIndexFiles())
                .extracting(IndexFileMeta::fileName)
                .containsExactly("btree-2", "bitmap-2");
    }

    private BucketedSortedIndexMaintainer sortedMaintainer(
            int fieldId,
            String indexType,
            DataFileMeta source,
            BucketedSortedIndexMaintainer.BuildFunction buildFunction) {
        return new BucketedSortedIndexMaintainer(
                fieldId,
                indexType,
                new PkSortedIndexFile(LocalFileIO.create(), pathFactory()),
                buildFunction,
                Collections.emptyList(),
                Collections.emptyList(),
                buildExecutor);
    }

    private static void enterBuild(AtomicInteger activeBuilds, AtomicInteger peakBuilds) {
        int active = activeBuilds.incrementAndGet();
        while (true) {
            int peak = peakBuilds.get();
            if (active <= peak || peakBuilds.compareAndSet(peak, active)) {
                return;
            }
        }
    }

    private static CompactIncrement compactAfter(DataFileMeta source) {
        return new CompactIncrement(
                Collections.emptyList(),
                Collections.singletonList(source),
                Collections.emptyList());
    }

    private static IndexFileMeta payload(
            String fileName,
            DataFileMeta sourceFile,
            long payloadRowCount,
            int fieldId,
            String indexType) {
        PrimaryKeyIndexSourceFile source =
                new PrimaryKeyIndexSourceFile(sourceFile.fileName(), sourceFile.rowCount());
        return new IndexFileMeta(
                indexType,
                fileName,
                1,
                payloadRowCount,
                new GlobalIndexMeta(
                        0,
                        source.rowCount() - 1,
                        fieldId,
                        null,
                        new byte[] {1},
                        new PrimaryKeyIndexSourceMeta(source).serialize()),
                null);
    }

    private static DataFileMeta dataFile(String fileName, long rowCount) {
        return DataFileMeta.forAppend(
                        fileName,
                        100,
                        rowCount,
                        SimpleStats.EMPTY_STATS,
                        0,
                        1,
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
}
