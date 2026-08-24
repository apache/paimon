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

package org.apache.paimon.index.pksorted;

import org.apache.paimon.fs.FileIO;
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
import org.apache.paimon.stats.SimpleStats;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests bucket-local BTree/Bitmap source maintenance. */
class BucketedSortedIndexMaintainerTest {

    @TempDir java.nio.file.Path tempPath;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @Test
    void testHasSingleConstructor() {
        assertThat(BucketedSortedIndexMaintainer.class.getDeclaredConstructors()).hasSize(1);
    }

    @AfterEach
    void shutdownExecutor() {
        executor.shutdownNow();
    }

    @Test
    void testRestoreBuildAndSourceRemoval() throws Exception {
        DataFileMeta oldSource = dataFile("data-1", 3);
        DataFileMeta newSource = dataFile("data-2", 3);
        List<IndexFileMeta> oldPayloads = Collections.singletonList(payload("old", oldSource, 3));
        IndexFileMeta newPayload = payload("new", newSource, 3);
        PkSortedIndexFile indexFile = new PkSortedIndexFile(LocalFileIO.create(), pathFactory());
        BucketedSortedIndexMaintainer maintainer =
                new BucketedSortedIndexMaintainer(
                        7,
                        "btree",
                        indexFile,
                        sourceFiles -> {
                            assertThat(sourceFiles).containsExactly(newSource);
                            return newPayload;
                        },
                        Collections.singletonList(oldSource),
                        oldPayloads,
                        executor);

        BucketedSortedIndexMaintainer.SortedIndexCommit commit =
                maintainer.prepareCommit(
                        DataIncrement.emptyIncrement(),
                        new CompactIncrement(
                                Collections.singletonList(oldSource),
                                Collections.singletonList(newSource),
                                Collections.emptyList()),
                        true);

        assertThat(commit.compactIncrement()).isPresent();
        assertThat(commit.compactIncrement().get().newIndexFiles()).containsExactly(newPayload);
        assertThat(commit.compactIncrement().get().deletedIndexFiles())
                .containsExactlyElementsOf(oldPayloads);
        assertThat(maintainer.state().coveredSourceFiles())
                .containsExactly(new PrimaryKeyIndexSourceFile("data-2", 3));
        assertThat(maintainer.state().uncoveredSourceFiles()).isEmpty();
    }

    @Test
    void testRebuildsDuplicateLevelPayloadsAsCompleteLevel() throws Exception {
        DataFileMeta sourceA = dataFile("data-a", 3);
        DataFileMeta sourceB = dataFile("data-b", 3);
        IndexFileMeta payloadA = payload("index-a", sourceA, 3);
        IndexFileMeta payloadB = payload("index-b", sourceB, 3);
        IndexFileMeta merged = payload("index-ab", Arrays.asList(sourceA, sourceB), 6);
        BucketedSortedIndexMaintainer maintainer =
                new BucketedSortedIndexMaintainer(
                        7,
                        "btree",
                        new PkSortedIndexFile(LocalFileIO.create(), pathFactory()),
                        sources -> {
                            assertThat(sources).containsExactly(sourceA, sourceB);
                            return merged;
                        },
                        Arrays.asList(sourceA, sourceB),
                        Arrays.asList(payloadA, payloadB),
                        executor);

        BucketedSortedIndexMaintainer.SortedIndexCommit commit =
                maintainer.prepareCommit(
                        DataIncrement.emptyIncrement(), CompactIncrement.emptyIncrement(), true);

        assertThat(commit.appendIncrement()).isPresent();
        assertThat(commit.appendIncrement().get().newIndexFiles()).containsExactly(merged);
        assertThat(commit.appendIncrement().get().deletedIndexFiles())
                .containsExactly(payloadA, payloadB);
        assertThat(maintainer.state().groups()).hasSize(1);
        assertThat(maintainer.state().groups().get(0).sourceFiles())
                .containsExactly(
                        new PrimaryKeyIndexSourceFile("data-a", 3),
                        new PrimaryKeyIndexSourceFile("data-b", 3));
    }

    @Test
    void testCoveredFanoutIsReportedAsPendingMaintenance() {
        DataFileMeta sourceA = dataFile("data-a", 3);
        DataFileMeta sourceB = dataFile("data-b", 3);
        BucketedSortedIndexMaintainer maintainer =
                new BucketedSortedIndexMaintainer(
                        7,
                        "btree",
                        new PkSortedIndexFile(LocalFileIO.create(), pathFactory()),
                        sources -> {
                            throw new AssertionError("Pending build must not start.");
                        },
                        Arrays.asList(sourceA, sourceB),
                        Arrays.asList(
                                payload("index-a", sourceA, 3), payload("index-b", sourceB, 3)),
                        executor);

        assertThat(maintainer.hasPendingMaintenance()).isTrue();
    }

    @Test
    void testPartiallyStaleGroupRebuildsOnlyActiveSources() throws Exception {
        DataFileMeta staleSource = dataFile("data-a", 3);
        DataFileMeta activeSource = dataFile("data-b", 7);
        IndexFileMeta oldPayload =
                payload("index-ab", Arrays.asList(staleSource, activeSource), 10);
        IndexFileMeta rebuiltPayload = payload("index-b", activeSource, 7);
        BucketedSortedIndexMaintainer maintainer =
                new BucketedSortedIndexMaintainer(
                        7,
                        "btree",
                        new PkSortedIndexFile(LocalFileIO.create(), pathFactory()),
                        sourceFiles -> {
                            assertThat(sourceFiles).containsExactly(activeSource);
                            return rebuiltPayload;
                        },
                        Arrays.asList(staleSource, activeSource),
                        Collections.singletonList(oldPayload),
                        executor);

        BucketedSortedIndexMaintainer.SortedIndexCommit commit =
                maintainer.prepareCommit(
                        DataIncrement.emptyIncrement(),
                        new CompactIncrement(
                                Collections.singletonList(staleSource),
                                Collections.emptyList(),
                                Collections.emptyList()),
                        true);

        assertThat(commit.compactIncrement()).isPresent();
        assertThat(commit.compactIncrement().get().newIndexFiles()).containsExactly(rebuiltPayload);
        assertThat(commit.compactIncrement().get().deletedIndexFiles()).containsExactly(oldPayload);
        assertThat(maintainer.state().groups()).hasSize(1);
        assertThat(maintainer.state().groups().get(0).sourceFiles())
                .containsExactly(new PrimaryKeyIndexSourceFile("data-b", 7));
    }

    @Test
    void testAllStaleGroupIsDeletedWithoutBuild() throws Exception {
        DataFileMeta source = dataFile("data-a", 3);
        IndexFileMeta oldPayload = payload("index-a", source, 3);
        AtomicInteger builds = new AtomicInteger();
        BucketedSortedIndexMaintainer maintainer =
                new BucketedSortedIndexMaintainer(
                        7,
                        "btree",
                        new PkSortedIndexFile(LocalFileIO.create(), pathFactory()),
                        sourceFiles -> {
                            builds.incrementAndGet();
                            return oldPayload;
                        },
                        Collections.singletonList(source),
                        Collections.singletonList(oldPayload),
                        executor);

        BucketedSortedIndexMaintainer.SortedIndexCommit commit =
                maintainer.prepareCommit(
                        DataIncrement.emptyIncrement(),
                        new CompactIncrement(
                                Collections.singletonList(source),
                                Collections.emptyList(),
                                Collections.emptyList()),
                        true);

        assertThat(builds).hasValue(0);
        assertThat(commit.compactIncrement()).isPresent();
        assertThat(commit.compactIncrement().get().newIndexFiles()).isEmpty();
        assertThat(commit.compactIncrement().get().deletedIndexFiles()).containsExactly(oldPayload);
        assertThat(maintainer.state().groups()).isEmpty();
    }

    @Test
    void testBlockingBuildsCompleteLevelWithoutIntermediates() throws Exception {
        List<DataFileMeta> sources =
                Arrays.asList(
                        dataFile("data-a", 3),
                        dataFile("data-b", 3),
                        dataFile("data-c", 3),
                        dataFile("data-d", 3));
        AtomicInteger generation = new AtomicInteger();
        List<IndexFileMeta> generated = new java.util.ArrayList<>();
        TrackingPkSortedIndexFile indexFile =
                new TrackingPkSortedIndexFile(LocalFileIO.create(), pathFactory());
        BucketedSortedIndexMaintainer maintainer =
                new BucketedSortedIndexMaintainer(
                        7,
                        "btree",
                        indexFile,
                        sourceFiles -> {
                            IndexFileMeta payload =
                                    payload(
                                            "index-" + generation.incrementAndGet(),
                                            sourceFiles,
                                            sourceFiles.size() * 3L);
                            generated.add(payload);
                            return payload;
                        },
                        Collections.emptyList(),
                        Collections.emptyList(),
                        executor);

        BucketedSortedIndexMaintainer.SortedIndexCommit commit =
                maintainer.prepareCommit(
                        DataIncrement.emptyIncrement(),
                        new CompactIncrement(
                                Collections.emptyList(), sources, Collections.emptyList()),
                        true);

        assertThat(generated).hasSize(1);
        assertThat(commit.compactIncrement()).isPresent();
        assertThat(commit.compactIncrement().get().newIndexFiles())
                .containsExactly(generated.get(0));
        assertThat(commit.compactIncrement().get().deletedIndexFiles()).isEmpty();
        assertThat(indexFile.deleted()).isEmpty();
    }

    @Test
    void testRestoreDeletesInvalidPayloadsBeforePublishingReplacement() throws Exception {
        DataFileMeta source = dataFile("data-1", 3);
        List<IndexFileMeta> invalidPayloads =
                Collections.singletonList(payload("invalid", source, 2));
        IndexFileMeta replacementPayload = payload("new", source, 3);
        PkSortedIndexFile indexFile = new PkSortedIndexFile(LocalFileIO.create(), pathFactory());
        BucketedSortedIndexMaintainer maintainer =
                new BucketedSortedIndexMaintainer(
                        7,
                        "btree",
                        indexFile,
                        sourceFiles -> replacementPayload,
                        Collections.singletonList(source),
                        invalidPayloads,
                        executor);

        BucketedSortedIndexMaintainer.SortedIndexCommit repair =
                maintainer.prepareCommit(
                        DataIncrement.emptyIncrement(), CompactIncrement.emptyIncrement(), true);

        assertThat(repair.appendIncrement()).isPresent();
        assertThat(repair.appendIncrement().get().deletedIndexFiles())
                .containsExactlyElementsOf(invalidPayloads);
        assertThat(repair.appendIncrement().get().newIndexFiles())
                .containsExactly(replacementPayload);

        BucketedSortedIndexMaintainer restored =
                new BucketedSortedIndexMaintainer(
                        7,
                        "btree",
                        indexFile,
                        sourceFiles -> {
                            throw new AssertionError("Covered source must not be rebuilt.");
                        },
                        Collections.singletonList(source),
                        Collections.singletonList(replacementPayload),
                        executor);
        BucketedSortedIndexMaintainer.SortedIndexCommit stable =
                restored.prepareCommit(
                        DataIncrement.emptyIncrement(),
                        CompactIncrement.emptyIncrement(),
                        false,
                        false);

        assertThat(stable.appendIncrement()).isEmpty();
        assertThat(stable.compactIncrement()).isEmpty();
        assertThat(restored.state().coveredSourceFiles())
                .containsExactly(new PrimaryKeyIndexSourceFile("data-1", 3));
    }

    @Test
    void testFinalBuildFailureThrowsAndRollsBackSourceTransition() {
        DataFileMeta oldSource = dataFile("data-1", 3);
        DataFileMeta newSource = dataFile("data-2", 3);
        List<IndexFileMeta> oldPayloads = Collections.singletonList(payload("old", oldSource, 3));
        AtomicInteger attempts = new AtomicInteger();
        BucketedSortedIndexMaintainer maintainer =
                new BucketedSortedIndexMaintainer(
                        7,
                        "btree",
                        new PkSortedIndexFile(LocalFileIO.create(), pathFactory()),
                        sourceFiles -> {
                            attempts.incrementAndGet();
                            throw new IllegalStateException("expected build failure");
                        },
                        Collections.singletonList(oldSource),
                        oldPayloads,
                        executor);

        assertThatThrownBy(
                        () ->
                                maintainer.prepareCommit(
                                        DataIncrement.emptyIncrement(),
                                        new CompactIncrement(
                                                Collections.singletonList(oldSource),
                                                Collections.singletonList(newSource),
                                                Collections.emptyList()),
                                        true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("expected build failure");

        assertThat(attempts).hasValue(3);
        assertThat(maintainer.state().coveredSourceFiles())
                .containsExactly(new PrimaryKeyIndexSourceFile("data-1", 3));
        assertThat(maintainer.state().uncoveredSourceFiles()).isEmpty();
    }

    @Test
    void testTransientFailureRetriesAndPublishesWholeGroup() throws Exception {
        DataFileMeta source = dataFile("data-1", 3);
        IndexFileMeta payload = payload("new", source, 3);
        AtomicInteger attempts = new AtomicInteger();
        BucketedSortedIndexMaintainer maintainer =
                new BucketedSortedIndexMaintainer(
                        7,
                        "btree",
                        new PkSortedIndexFile(LocalFileIO.create(), pathFactory()),
                        sourceFiles -> {
                            if (attempts.incrementAndGet() < 3) {
                                throw new IllegalStateException("expected transient failure");
                            }
                            return payload;
                        },
                        Collections.emptyList(),
                        Collections.emptyList(),
                        executor);

        BucketedSortedIndexMaintainer.SortedIndexCommit commit =
                maintainer.prepareCommit(
                        DataIncrement.emptyIncrement(), compactAfter(source), true);

        assertThat(attempts).hasValue(3);
        assertThat(commit.compactIncrement()).isPresent();
        assertThat(commit.compactIncrement().get().newIndexFiles()).containsExactly(payload);
        assertThat(maintainer.state().coveredSourceFiles())
                .containsExactly(new PrimaryKeyIndexSourceFile("data-1", 3));
    }

    @Test
    void testNonBlockingBuildPublishesOnLaterCommit() throws Exception {
        DataFileMeta source = dataFile("data-1", 3);
        IndexFileMeta payload = payload("new", source, 3);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        BucketedSortedIndexMaintainer maintainer =
                new BucketedSortedIndexMaintainer(
                        7,
                        "btree",
                        new PkSortedIndexFile(LocalFileIO.create(), pathFactory()),
                        sourceFiles -> {
                            started.countDown();
                            release.await();
                            return payload;
                        },
                        Collections.emptyList(),
                        Collections.emptyList(),
                        executor);

        BucketedSortedIndexMaintainer.SortedIndexCommit first =
                maintainer.prepareCommit(
                        DataIncrement.emptyIncrement(), compactAfter(source), false);
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(first.compactIncrement()).isEmpty();
        assertThat(maintainer.buildNotCompleted()).isTrue();

        release.countDown();
        executor.submit(() -> {}).get(5, TimeUnit.SECONDS);
        BucketedSortedIndexMaintainer.SortedIndexCommit second =
                maintainer.prepareCommit(
                        DataIncrement.emptyIncrement(), CompactIncrement.emptyIncrement(), false);

        assertThat(second.appendIncrement()).isPresent();
        assertThat(second.appendIncrement().get().newIndexFiles()).containsExactly(payload);
        assertThat(maintainer.buildNotCompleted()).isFalse();
    }

    @Test
    void testDoesNotPublishPayloadForDifferentDataLevel() throws Exception {
        DataFileMeta source = dataFile("data-1", 3);
        IndexFileMeta wrongLevelPayload =
                payload("wrong-level", Collections.singletonList(source), 3, 2);
        TrackingPkSortedIndexFile indexFile =
                new TrackingPkSortedIndexFile(LocalFileIO.create(), pathFactory());
        BucketedSortedIndexMaintainer maintainer =
                new BucketedSortedIndexMaintainer(
                        7,
                        "btree",
                        indexFile,
                        sourceFiles -> wrongLevelPayload,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        executor);

        maintainer.prepareCommit(DataIncrement.emptyIncrement(), compactAfter(source), false, true);
        executor.submit(() -> {}).get(5, TimeUnit.SECONDS);
        BucketedSortedIndexMaintainer.SortedIndexCommit commit =
                maintainer.prepareCommit(
                        DataIncrement.emptyIncrement(),
                        CompactIncrement.emptyIncrement(),
                        false,
                        false);

        assertThat(commit.appendIncrement()).isEmpty();
        assertThat(indexFile.deleted()).containsExactly(wrongLevelPayload);
    }

    @Test
    void testStaleCompletionIsDeletedBeforeReplacementPublishes() throws Exception {
        DataFileMeta staleSource = dataFile("data-1", 3);
        DataFileMeta activeSource = dataFile("data-2", 3);
        IndexFileMeta stalePayload = payload("stale", staleSource, 3);
        IndexFileMeta activePayload = payload("active", activeSource, 3);
        CountDownLatch staleStarted = new CountDownLatch(1);
        CountDownLatch releaseStale = new CountDownLatch(1);
        CountDownLatch staleCompleted = new CountDownLatch(1);
        TrackingPkSortedIndexFile indexFile =
                new TrackingPkSortedIndexFile(LocalFileIO.create(), pathFactory());
        BucketedSortedIndexMaintainer maintainer =
                new BucketedSortedIndexMaintainer(
                        7,
                        "btree",
                        indexFile,
                        sourceFiles -> {
                            DataFileMeta sourceFile = sourceFiles.get(0);
                            if (sourceFile.fileName().equals(staleSource.fileName())) {
                                staleStarted.countDown();
                                releaseStale.await();
                                staleCompleted.countDown();
                                return stalePayload;
                            }
                            assertThat(sourceFile).isEqualTo(activeSource);
                            return activePayload;
                        },
                        Collections.emptyList(),
                        Collections.emptyList(),
                        executor);

        maintainer.prepareCommit(DataIncrement.emptyIncrement(), compactAfter(staleSource), false);
        assertThat(staleStarted.await(5, TimeUnit.SECONDS)).isTrue();
        maintainer.prepareCommit(
                DataIncrement.emptyIncrement(),
                new CompactIncrement(
                        Collections.singletonList(staleSource),
                        Collections.singletonList(activeSource),
                        Collections.emptyList()),
                false);
        releaseStale.countDown();
        assertThat(staleCompleted.await(5, TimeUnit.SECONDS)).isTrue();

        BucketedSortedIndexMaintainer.SortedIndexCommit commit =
                maintainer.prepareCommit(
                        DataIncrement.emptyIncrement(), CompactIncrement.emptyIncrement(), true);

        assertThat(indexFile.deleted()).containsExactly(stalePayload);
        assertThat(commit.appendIncrement()).isPresent();
        assertThat(commit.appendIncrement().get().newIndexFiles()).containsExactly(activePayload);
        assertThat(maintainer.state().coveredSourceFiles())
                .containsExactly(new PrimaryKeyIndexSourceFile("data-2", 3));
    }

    @Test
    void testLevelCompletionIsDiscardedWhenPlannedSourceRetires() throws Exception {
        DataFileMeta sourceA = dataFile("data-a", 3);
        DataFileMeta sourceB = dataFile("data-b", 3);
        IndexFileMeta payloadA = payload("index-a", sourceA, 3);
        IndexFileMeta staleMerged = payload("index-ab", Arrays.asList(sourceA, sourceB), 6);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        TrackingPkSortedIndexFile indexFile =
                new TrackingPkSortedIndexFile(LocalFileIO.create(), pathFactory());
        BucketedSortedIndexMaintainer maintainer =
                new BucketedSortedIndexMaintainer(
                        7,
                        "btree",
                        indexFile,
                        sourceFiles -> {
                            started.countDown();
                            release.await();
                            return staleMerged;
                        },
                        Collections.singletonList(sourceA),
                        Collections.singletonList(payloadA),
                        executor);

        maintainer.prepareCommit(DataIncrement.emptyIncrement(), compactAfter(sourceB), false);
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
        maintainer.prepareCommit(
                DataIncrement.emptyIncrement(),
                new CompactIncrement(
                        Collections.singletonList(sourceB),
                        Collections.emptyList(),
                        Collections.emptyList()),
                false);
        release.countDown();
        executor.submit(() -> {}).get(5, TimeUnit.SECONDS);

        BucketedSortedIndexMaintainer.SortedIndexCommit cleanup =
                maintainer.prepareCommit(
                        DataIncrement.emptyIncrement(), CompactIncrement.emptyIncrement(), true);

        assertThat(indexFile.deleted()).contains(staleMerged);
        assertThat(cleanup.appendIncrement()).isEmpty();
        assertThat(maintainer.state().groups()).hasSize(1);
        assertThat(maintainer.state().groups().get(0).payloads()).containsExactly(payloadA);
    }

    @Test
    void testRejectedSubmissionThrowsAndRollsBackSourceTransition() {
        DataFileMeta source = dataFile("data-1", 3);
        ExecutorService rejectedExecutor = Executors.newSingleThreadExecutor();
        rejectedExecutor.shutdownNow();
        BucketedSortedIndexMaintainer maintainer =
                new BucketedSortedIndexMaintainer(
                        7,
                        "btree",
                        new PkSortedIndexFile(LocalFileIO.create(), pathFactory()),
                        sourceFiles -> payload("new", source, 3),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        rejectedExecutor);

        assertThatThrownBy(
                        () ->
                                maintainer.prepareCommit(
                                        DataIncrement.emptyIncrement(),
                                        compactAfter(source),
                                        false))
                .isInstanceOf(java.util.concurrent.RejectedExecutionException.class);

        assertThat(maintainer.buildNotCompleted()).isFalse();
        assertThat(maintainer.state().uncoveredSourceFiles()).isEmpty();
    }

    @Test
    void testMalformedOutputThrowsAndRollsBackSourceTransition() {
        DataFileMeta source = dataFile("data-1", 3);
        IndexFileMeta malformed = payload("malformed", source, 2);
        TrackingPkSortedIndexFile indexFile =
                new TrackingPkSortedIndexFile(LocalFileIO.create(), pathFactory());
        BucketedSortedIndexMaintainer maintainer =
                new BucketedSortedIndexMaintainer(
                        7,
                        "btree",
                        indexFile,
                        sourceFiles -> malformed,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        executor);

        assertThatThrownBy(
                        () ->
                                maintainer.prepareCommit(
                                        DataIncrement.emptyIncrement(), compactAfter(source), true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("incomplete group");

        assertThat(indexFile.deleted()).containsExactly(malformed);
        assertThat(maintainer.state().groups()).isEmpty();
        assertThat(maintainer.state().uncoveredSourceFiles()).isEmpty();
    }

    @Test
    void testCloseDeletesResultCompletedAfterCancellation() throws Exception {
        DataFileMeta source = dataFile("data-1", 3);
        IndexFileMeta generated = payload("cancelled", source, 3);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        TrackingPkSortedIndexFile indexFile =
                new TrackingPkSortedIndexFile(LocalFileIO.create(), pathFactory());
        BucketedSortedIndexMaintainer maintainer =
                new BucketedSortedIndexMaintainer(
                        7,
                        "btree",
                        indexFile,
                        sourceFiles -> {
                            started.countDown();
                            boolean released = false;
                            while (!released) {
                                try {
                                    release.await();
                                    released = true;
                                } catch (InterruptedException ignored) {
                                    // Simulate a builder completing despite cancellation.
                                }
                            }
                            return generated;
                        },
                        Collections.emptyList(),
                        Collections.emptyList(),
                        executor);

        maintainer.prepareCommit(DataIncrement.emptyIncrement(), compactAfter(source), false);
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
        maintainer.close();
        release.countDown();

        assertThat(indexFile.awaitDeletion()).isTrue();
        assertThat(indexFile.deleted()).containsExactly(generated);
        assertThat(maintainer.buildNotCompleted()).isFalse();
    }

    @Test
    void testInterruptedCommitRollsBackStateAndCancelsBuild() throws Exception {
        DataFileMeta oldSource = dataFile("data-1", 3);
        DataFileMeta newSource = dataFile("data-2", 3);
        List<IndexFileMeta> oldPayloads = Collections.singletonList(payload("old", oldSource, 3));
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch cancelled = new CountDownLatch(1);
        BucketedSortedIndexMaintainer maintainer =
                new BucketedSortedIndexMaintainer(
                        7,
                        "btree",
                        new PkSortedIndexFile(LocalFileIO.create(), pathFactory()),
                        sourceFiles -> {
                            started.countDown();
                            try {
                                new CountDownLatch(1).await();
                                throw new AssertionError("Build must be cancelled.");
                            } catch (InterruptedException e) {
                                cancelled.countDown();
                                throw e;
                            }
                        },
                        Collections.singletonList(oldSource),
                        oldPayloads,
                        executor);
        CompactIncrement transition =
                new CompactIncrement(
                        Collections.singletonList(oldSource),
                        Collections.singletonList(newSource),
                        Collections.emptyList());
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread commitThread =
                new Thread(
                        () -> {
                            try {
                                maintainer.prepareCommit(
                                        DataIncrement.emptyIncrement(), transition, true);
                            } catch (Throwable t) {
                                failure.set(t);
                            }
                        });

        commitThread.start();
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
        commitThread.interrupt();
        commitThread.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(commitThread.isAlive()).isFalse();
        assertThat(failure.get()).isInstanceOf(InterruptedException.class);
        assertThat(cancelled.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(maintainer.buildNotCompleted()).isFalse();
        assertThat(maintainer.state().coveredSourceFiles())
                .containsExactly(new PrimaryKeyIndexSourceFile("data-1", 3));
        assertThat(maintainer.state().uncoveredSourceFiles()).isEmpty();
    }

    private static CompactIncrement compactAfter(DataFileMeta source) {
        return new CompactIncrement(
                Collections.emptyList(),
                Collections.singletonList(source),
                Collections.emptyList());
    }

    private static IndexFileMeta payload(
            String fileName, DataFileMeta sourceFile, long payloadRowCount) {
        return payload(fileName, Collections.singletonList(sourceFile), payloadRowCount);
    }

    private static IndexFileMeta payload(
            String fileName, List<DataFileMeta> sourceFiles, long payloadRowCount) {
        return payload(fileName, sourceFiles, payloadRowCount, sourceFiles.get(0).level());
    }

    private static IndexFileMeta payload(
            String fileName, List<DataFileMeta> sourceFiles, long payloadRowCount, int dataLevel) {
        List<PrimaryKeyIndexSourceFile> sources = new java.util.ArrayList<>();
        long rowCount = 0;
        for (DataFileMeta sourceFile : sourceFiles) {
            sources.add(
                    new PrimaryKeyIndexSourceFile(sourceFile.fileName(), sourceFile.rowCount()));
            rowCount = Math.addExact(rowCount, sourceFile.rowCount());
        }
        return new IndexFileMeta(
                "btree",
                fileName,
                1,
                payloadRowCount,
                new GlobalIndexMeta(
                        0,
                        rowCount - 1,
                        7,
                        null,
                        new byte[] {1},
                        new PrimaryKeyIndexSourceMeta(dataLevel, sources).serialize()),
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

    private static class TrackingPkSortedIndexFile extends PkSortedIndexFile {

        private final List<IndexFileMeta> deleted = new java.util.ArrayList<>();
        private final CountDownLatch deletion = new CountDownLatch(1);

        private TrackingPkSortedIndexFile(FileIO fileIO, IndexPathFactory pathFactory) {
            super(fileIO, pathFactory);
        }

        @Override
        public synchronized void delete(IndexFileMeta file) {
            deleted.add(file);
            deletion.countDown();
        }

        private synchronized List<IndexFileMeta> deleted() {
            return new java.util.ArrayList<>(deleted);
        }

        private boolean awaitDeletion() throws InterruptedException {
            return deletion.await(1, TimeUnit.SECONDS);
        }
    }
}
