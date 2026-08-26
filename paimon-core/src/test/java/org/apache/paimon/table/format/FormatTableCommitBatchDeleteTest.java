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

package org.apache.paimon.table.format;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.BatchDeleteResult;
import org.apache.paimon.fs.BatchFileDeleter;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.paimon.CoreOptions.PARTITION_DEFAULT_NAME;
import static org.apache.paimon.table.format.FormatTableCommitTestUtils.failureTree;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Strict batch-delete consumer tests for {@link FormatTableCommit}. */
class FormatTableCommitBatchDeleteTest {

    private static final int OSS_BATCH_SIZE = 1000;
    private static final Identifier TABLE =
            Identifier.create("batch_delete_db", "batch_delete_table");

    @TempDir java.nio.file.Path tempDir;

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testChunks9384FilesSequentiallyAndWaitsForFinalBatchBeforePublishing() throws Exception {
        CountDownLatch finalBatchStartedOrCommitReturned = new CountDownLatch(1);
        Path tablePath = new Path(new Path(tempDir.toUri()), "large-batch");
        Path firstPartition = new Path(tablePath, "part=p0");
        Path secondPartition = new Path(tablePath, "part=p1");
        SuccessfulBatchFileIO fileIO = new SuccessfulBatchFileIO(finalBatchStartedOrCommitReturned);
        List<Path> oldFiles = new ArrayList<>();
        oldFiles.addAll(fileIO.addOldFiles(firstPartition, 9000));
        oldFiles.addAll(fileIO.addOldFiles(secondPartition, 384));
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        TrackingCommitter firstCommitter =
                new TrackingCommitter(new Path(firstPartition, "data-new.csv"), null);
        TrackingCommitter secondCommitter =
                new TrackingCommitter(new Path(secondPartition, "data-new.csv"), null);
        TrackingCommitMessage firstMessage = new TrackingCommitMessage(firstCommitter, 7, 123);
        TrackingCommitMessage secondMessage = new TrackingCommitMessage(secondCommitter, 11, 456);
        List<CommitMessage> messages = Arrays.asList(firstMessage, secondMessage);
        CountingExecutorService publishExecutor =
                new CountingExecutorService(Executors.newFixedThreadPool(2));
        FormatTableCommit commit =
                new FormatTableCommit(
                        tablePath.toString(),
                        Collections.singletonList("part"),
                        fileIO,
                        false,
                        PARTITION_DEFAULT_NAME.defaultValue(),
                        true,
                        TABLE,
                        null,
                        null,
                        null,
                        partitionManager,
                        true,
                        64,
                        2,
                        publishExecutor);

        ExecutorService callerExecutor = Executors.newSingleThreadExecutor();
        Future<?> result =
                callerExecutor.submit(
                        () -> {
                            try {
                                commit.commit(messages);
                            } finally {
                                fileIO.signalCommitReturned();
                            }
                        });
        try {
            assertThat(finalBatchStartedOrCommitReturned.await(30, TimeUnit.SECONDS)).isTrue();
            assertThat(fileIO.finalBatchStarted()).isTrue();
            assertThat(result.isDone()).isFalse();
            assertThat(fileIO.batchCalls()).isEqualTo(10);
            assertThat(fileIO.maxBatchSizeCalls()).isOne();
            assertThat(fileIO.maxConcurrentBatchCalls()).isOne();
            assertThat(publishExecutor.acceptedTasks()).isZero();
            assertThat(firstCommitter.commitCalls()).isZero();
            assertThat(secondCommitter.commitCalls()).isZero();
            assertThat(firstCommitter.cleanCalls()).isZero();
            assertThat(secondCommitter.cleanCalls()).isZero();
            assertThat(firstMessage.statisticsAccessCalls()).isZero();
            assertThat(secondMessage.statisticsAccessCalls()).isZero();
            verify(partitionManager, never())
                    .createPartitions(anyList(), eq(true), any(), anyBoolean());

            fileIO.releaseFinalBatch();
            result.get(30, TimeUnit.SECONDS);
        } finally {
            fileIO.releaseFinalBatch();
            callerExecutor.shutdownNow();
            publishExecutor.shutdownNow();
        }

        List<List<Path>> expectedBatches = chunks(oldFiles, OSS_BATCH_SIZE);
        assertThat(fileIO.discoveryPaths()).containsExactly(oldFiles.get(0));
        assertThat(fileIO.pathExistedAtDiscovery()).containsExactly(true);
        List<List<Path>> actualBatches = fileIO.batchInputs();
        assertThat(actualBatches).containsExactlyElementsOf(expectedBatches);
        assertThat(actualBatches.subList(0, 9))
                .allSatisfy(batch -> assertThat(batch).hasSize(1000));
        assertThat(actualBatches.get(9)).hasSize(384);
        Set<List<Path>> identities = Collections.newSetFromMap(new IdentityHashMap<>());
        for (List<Path> batch : actualBatches) {
            assertThat(identities.add(batch)).as("each batch is a fresh list").isTrue();
        }
        assertThat(fileIO.singleDeleteCalls()).isZero();
        assertThat(fileIO.listStatus(firstPartition)).isEmpty();
        assertThat(fileIO.listStatus(secondPartition)).isEmpty();
        assertThat(publishExecutor.acceptedTasks()).isEqualTo(2);
        assertThat(firstCommitter.commitCalls()).isOne();
        assertThat(secondCommitter.commitCalls()).isOne();
        assertThat(firstCommitter.cleanCalls()).isOne();
        assertThat(secondCommitter.cleanCalls()).isOne();
        assertThat(firstCommitter.discardCalls()).isZero();
        assertThat(secondCommitter.discardCalls()).isZero();
        assertThat(firstMessage.statisticsAccessCalls()).isEqualTo(2);
        assertThat(secondMessage.statisticsAccessCalls()).isEqualTo(2);

        ArgumentCaptor<List<Map<String, String>>> specs =
                ArgumentCaptor.forClass((Class) List.class);
        ArgumentCaptor<List<PartitionStatistics>> statistics =
                ArgumentCaptor.forClass((Class) List.class);
        verify(partitionManager)
                .createPartitions(specs.capture(), eq(true), statistics.capture(), eq(true));
        assertThat(specs.getValue())
                .containsExactlyInAnyOrder(
                        Collections.singletonMap("part", "p0"),
                        Collections.singletonMap("part", "p1"));
        assertThat(statistics.getValue())
                .hasSize(2)
                .anySatisfy(
                        stat -> {
                            assertThat(stat.spec())
                                    .isEqualTo(Collections.singletonMap("part", "p0"));
                            assertThat(stat.recordCount()).isEqualTo(7);
                            assertThat(stat.fileSizeInBytes()).isEqualTo(123);
                            assertThat(stat.fileCount()).isOne();
                        })
                .anySatisfy(
                        stat -> {
                            assertThat(stat.spec())
                                    .isEqualTo(Collections.singletonMap("part", "p1"));
                            assertThat(stat.recordCount()).isEqualTo(11);
                            assertThat(stat.fileSizeInBytes()).isEqualTo(456);
                            assertThat(stat.fileCount()).isOne();
                        });
    }

    @Test
    void testUnsupportedCapabilityPushesFirstFileBackIntoSingleDeleteCleanup() throws Exception {
        UnsupportedBatchFileIO fileIO = new UnsupportedBatchFileIO();
        Path tablePath = new Path(new Path(tempDir.toUri()), "unsupported");
        Path partitionPath = new Path(tablePath, "part=p");
        fileIO.rejectRelisting(partitionPath);
        List<Path> oldFiles = writeOldFiles(fileIO, partitionPath, 3);
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        TrackingCommitter committer =
                new TrackingCommitter(
                        new Path(partitionPath, "data-new.csv"),
                        () -> {
                            if (fileIO.activeSingleDeletes() != 0) {
                                throw new IOException("Publish overlapped fallback cleanup");
                            }
                            for (Path oldFile : oldFiles) {
                                if (fileIO.exists(oldFile)) {
                                    throw new IOException("Old file survived fallback: " + oldFile);
                                }
                            }
                        });

        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<?> result =
                    caller.submit(
                            () ->
                                    dynamicOverwrite(tablePath, fileIO, partitionManager, 2)
                                            .commit(
                                                    Collections.singletonList(
                                                            new TwoPhaseCommitMessage(
                                                                    committer, 1, 1))));

            fileIO.awaitBothSingleDeletesStarted();
            assertThat(fileIO.discoveryPaths()).containsExactly(oldFiles.get(0));
            assertThat(fileIO.pathExistedAtDiscovery()).containsExactly(true);
            assertThat(fileIO.singleDeletePaths())
                    .hasSize(2)
                    .doesNotHaveDuplicates()
                    .isSubsetOf(oldFiles);
            assertThat(fileIO.activeSingleDeletes()).isEqualTo(2);
            assertThat(fileIO.maxConcurrentSingleDeletes()).isEqualTo(2);
            assertThat(committer.commitCalls()).isZero();
            assertThat(committer.cleanCalls()).isZero();

            fileIO.releaseSingleDeletes();
            result.get(10, TimeUnit.SECONDS);
        } finally {
            fileIO.releaseSingleDeletes();
            caller.shutdownNow();
        }

        assertThat(fileIO.discoveryPaths()).containsExactly(oldFiles.get(0));
        assertThat(fileIO.pathExistedAtDiscovery()).containsExactly(true);
        assertThat(fileIO.singleDeletePaths())
                .containsExactlyInAnyOrderElementsOf(oldFiles)
                .hasSize(oldFiles.size());
        assertThat(fileIO.recursiveDeleteArguments()).containsOnly(false);
        assertThat(fileIO.partitionListings()).isOne();
        assertThat(fileIO.maxConcurrentSingleDeletes()).isEqualTo(2);
        assertThat(committer.commitCalls()).isOne();
        assertThat(committer.discardCalls()).isZero();
    }

    @Test
    void testLaterRootListingFailureDoesNotSendPartialBatch() throws Exception {
        Path tablePath = new Path(new Path(tempDir.toUri()), "later-listing-failure");
        Path firstPartition = new Path(tablePath, "part=p0");
        Path failingPartition = new Path(tablePath, "part=p1");
        LaterRootListingFailureFileIO fileIO = new LaterRootListingFailureFileIO(failingPartition);
        Path oldFile = writeOldFiles(fileIO, firstPartition, 1).get(0);
        fileIO.mkdirs(failingPartition);
        TrackingCommitter firstCommitter =
                new TrackingCommitter(new Path(firstPartition, "data-new.csv"), null);
        TrackingCommitter secondCommitter =
                new TrackingCommitter(new Path(failingPartition, "data-new.csv"), null);

        Throwable failure =
                catchThrowable(
                        () ->
                                dynamicOverwrite(
                                                tablePath,
                                                fileIO,
                                                mock(FormatTablePartitionManager.class),
                                                2)
                                        .commit(
                                                Arrays.asList(
                                                        new TwoPhaseCommitMessage(firstCommitter),
                                                        new TwoPhaseCommitMessage(
                                                                secondCommitter))));

        assertThat(failure).isNotNull();
        assertThat(fileIO.discoveryPaths()).containsExactly(oldFile);
        assertThat(fileIO.maxBatchSizeCalls()).isOne();
        assertThat(fileIO.batchCalls()).isZero();
        assertThat(fileIO.singleDeleteCalls()).isZero();
        assertThat(fileIO.exists(oldFile)).isTrue();
        assertThat(firstCommitter.commitCalls()).isZero();
        assertThat(secondCommitter.commitCalls()).isZero();
        assertThat(firstCommitter.cleanCalls()).isZero();
        assertThat(secondCommitter.cleanCalls()).isZero();
        assertThat(firstCommitter.discardCalls()).isOne();
        assertThat(secondCommitter.discardCalls()).isOne();
    }

    @Test
    void testEmptyWrittenPartitionDoesNotDiscoverBatchCapability() throws Exception {
        RejectingDiscoveryFileIO fileIO = new RejectingDiscoveryFileIO();
        Path tablePath = new Path(new Path(tempDir.toUri()), "empty-partition");
        Path partitionPath = new Path(tablePath, "part=p");
        fileIO.mkdirs(partitionPath);
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        TrackingCommitter committer =
                new TrackingCommitter(new Path(partitionPath, "data-new.csv"), null);

        dynamicOverwrite(tablePath, fileIO, partitionManager, 2)
                .commit(Collections.singletonList(new TwoPhaseCommitMessage(committer)));

        assertThat(fileIO.discoveryCalls()).isZero();
        assertThat(fileIO.singleDeleteCalls()).isZero();
        assertThat(committer.commitCalls()).isOne();
    }

    @ParameterizedTest
    @EnumSource(ExcludedMode.class)
    void testExcludedModesNeverDiscoverBatchCapability(ExcludedMode mode) throws Exception {
        RejectingDiscoveryFileIO fileIO = new RejectingDiscoveryFileIO();
        Path tablePath = new Path(new Path(tempDir.toUri()), "excluded-" + mode.name());
        Path partitionPath =
                mode == ExcludedMode.UNPARTITIONED
                        ? tablePath
                        : mode == ExcludedMode.STATIC_PREFIX
                                ? new Path(tablePath, "year=2025/month=10")
                                : new Path(tablePath, "part=p");
        Path oldFile = writeOldFiles(fileIO, partitionPath, 1).get(0);
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        TrackingCommitter committer =
                new TrackingCommitter(new Path(partitionPath, "data-new.csv"), null);

        runExcludedMode(mode, tablePath, fileIO, partitionManager, committer);

        assertThat(fileIO.discoveryCalls()).as(mode.name()).isZero();
        if (mode == ExcludedMode.APPEND) {
            assertThat(fileIO.singleDeleteCalls()).as(mode.name()).isZero();
            assertThat(fileIO.exists(oldFile)).as(mode.name()).isTrue();
        } else {
            assertThat(fileIO.singleDeleteCalls()).as(mode.name()).isOne();
            assertThat(fileIO.exists(oldFile)).as(mode.name()).isFalse();
        }
    }

    @Test
    void testNullPartitionManagerNeverDiscoversCapabilityWithConcurrentCleanup() throws Exception {
        RejectingDiscoveryFileIO fileIO = new RejectingDiscoveryFileIO();
        Path tablePath = new Path(new Path(tempDir.toUri()), "null-partition-manager");
        Path partitionPath = new Path(tablePath, "part=p");
        Path oldFile = writeOldFiles(fileIO, partitionPath, 1).get(0);
        TrackingCommitter committer =
                new TrackingCommitter(new Path(partitionPath, "data-new.csv"), null);

        new FormatTableCommit(
                        tablePath.toString(),
                        Collections.singletonList("part"),
                        fileIO,
                        false,
                        PARTITION_DEFAULT_NAME.defaultValue(),
                        true,
                        TABLE,
                        null,
                        null,
                        null,
                        null,
                        true,
                        2)
                .commit(Collections.singletonList(new TwoPhaseCommitMessage(committer)));

        assertThat(fileIO.discoveryCalls()).isZero();
        assertThat(fileIO.singleDeleteCalls()).isOne();
        assertThat(fileIO.exists(oldFile)).isFalse();
        assertThat(committer.commitCalls()).isOne();
        assertThat(committer.cleanCalls()).isOne();
        assertThat(committer.discardCalls()).isZero();
    }

    @ParameterizedTest
    @EnumSource(DiscoveryFailure.class)
    void testCapabilityDiscoveryErrorsAreHardFailures(DiscoveryFailure outcome) throws Exception {
        DiscoveryFailureFileIO fileIO = new DiscoveryFailureFileIO(outcome);
        Path tablePath = new Path(new Path(tempDir.toUri()), "discovery-" + outcome.name());
        Path partitionPath = new Path(tablePath, "part=p");
        Path oldFile = writeOldFiles(fileIO, partitionPath, 1).get(0);
        TrackingCommitter committer =
                new TrackingCommitter(new Path(partitionPath, "data-new.csv"), null);

        Throwable failure =
                catchThrowable(
                        () ->
                                dynamicOverwrite(
                                                tablePath,
                                                fileIO,
                                                mock(FormatTablePartitionManager.class),
                                                2)
                                        .commit(
                                                Collections.singletonList(
                                                        new TwoPhaseCommitMessage(committer))));

        assertThat(failure).as(outcome.name()).isNotNull();
        assertThat(fileIO.discoveryPaths()).containsExactly(oldFile);
        assertThat(fileIO.pathExistedAtDiscovery()).containsExactly(true);
        assertThat(fileIO.singleDeleteCalls()).isZero();
        assertThat(fileIO.batchCalls()).isZero();
        assertThat(fileIO.exists(oldFile)).isTrue();
        assertThat(committer.commitCalls()).isZero();
        assertThat(committer.cleanCalls()).isZero();
        assertThat(committer.discardCalls()).isOne();
    }

    @ParameterizedTest
    @EnumSource(MaxBatchSizeFailure.class)
    void testInvalidProviderBatchSizeIsHardFailure(MaxBatchSizeFailure outcome) throws Exception {
        MaxBatchSizeFailureFileIO fileIO = new MaxBatchSizeFailureFileIO(outcome);
        Path tablePath = new Path(new Path(tempDir.toUri()), "max-size-" + outcome.name());
        Path partitionPath = new Path(tablePath, "part=p");
        Path oldFile = writeOldFiles(fileIO, partitionPath, 1).get(0);
        TrackingCommitter committer =
                new TrackingCommitter(new Path(partitionPath, "data-new.csv"), null);

        Throwable failure =
                catchThrowable(
                        () ->
                                dynamicOverwrite(
                                                tablePath,
                                                fileIO,
                                                mock(FormatTablePartitionManager.class),
                                                2)
                                        .commit(
                                                Collections.singletonList(
                                                        new TwoPhaseCommitMessage(committer))));

        assertThat(failure).as(outcome.name()).isNotNull();
        assertThat(fileIO.discoveryPaths()).containsExactly(oldFile);
        assertThat(fileIO.maxBatchSizeCalls()).isOne();
        assertThat(fileIO.batchCalls()).isZero();
        assertThat(fileIO.singleDeleteCalls()).isZero();
        assertThat(fileIO.exists(oldFile)).isTrue();
        assertThat(committer.commitCalls()).isZero();
        assertThat(committer.cleanCalls()).isZero();
        assertThat(committer.discardCalls()).isOne();
    }

    @Test
    void testMaximumProviderBatchSizeDoesNotPreallocateRequestList() throws Exception {
        MaximumBatchSizeFileIO fileIO = new MaximumBatchSizeFileIO();
        Path tablePath = new Path(new Path(tempDir.toUri()), "maximum-batch-size");
        Path partitionPath = new Path(tablePath, "part=p");
        List<Path> oldFiles = writeOldFiles(fileIO, partitionPath, 3);
        TrackingCommitter committer =
                new TrackingCommitter(new Path(partitionPath, "data-new.csv"), null);

        dynamicOverwrite(tablePath, fileIO, mock(FormatTablePartitionManager.class), 2)
                .commit(Collections.singletonList(new TwoPhaseCommitMessage(committer)));

        assertThat(fileIO.discoveryPaths()).containsExactly(oldFiles.get(0));
        assertThat(fileIO.maxBatchSizeCalls()).isOne();
        assertThat(fileIO.batchCalls()).isOne();
        assertThat(fileIO.singleDeleteCalls()).isZero();
        assertThat(fileIO.listStatus(partitionPath)).isEmpty();
        assertThat(committer.commitCalls()).isOne();
        assertThat(committer.cleanCalls()).isOne();
        assertThat(committer.discardCalls()).isZero();
    }

    @ParameterizedTest
    @EnumSource(BatchOutcome.class)
    void testDeleteAndResultErrorsNeverFallBackOrPublish(BatchOutcome outcome) throws Exception {
        StrictOutcomeFileIO fileIO = new StrictOutcomeFileIO(outcome);
        Path tablePath = new Path(new Path(tempDir.toUri()), "strict-" + outcome.name());
        Path partitionPath = new Path(tablePath, "part=p");
        List<Path> oldFiles = writeOldFiles(fileIO, partitionPath, 2);
        TrackingCommitter committer =
                new TrackingCommitter(new Path(partitionPath, "data-new.csv"), null);

        Throwable failure =
                catchThrowable(
                        () ->
                                dynamicOverwrite(
                                                tablePath,
                                                fileIO,
                                                mock(FormatTablePartitionManager.class),
                                                2)
                                        .commit(
                                                Collections.singletonList(
                                                        new TwoPhaseCommitMessage(committer))));

        assertThat(failure).as(outcome.name()).isNotNull();
        assertThat(fileIO.discoveryPaths()).containsExactly(oldFiles.get(0));
        assertThat(fileIO.maxBatchSizeCalls()).isOne();
        assertThat(fileIO.batchCalls()).isOne();
        assertThat(fileIO.batchInputs()).containsExactlyElementsOf(oldFiles);
        assertThat(fileIO.singleDeleteCalls()).isZero();
        assertThat(committer.commitCalls()).isZero();
        assertThat(committer.cleanCalls()).isZero();
        assertThat(committer.discardCalls()).isOne();
        if (outcome == BatchOutcome.PARTIAL_DELETE_THEN_THROW) {
            assertThat(fileIO.exists(oldFiles.get(0))).isFalse();
            assertThat(fileIO.exists(oldFiles.get(1))).isTrue();
        }
    }

    @Test
    void testSecondBatchFailureStopsThirdBatchAndAbortsBeforePublish() throws Exception {
        SecondBatchFailureFileIO fileIO = new SecondBatchFailureFileIO();
        Path tablePath = new Path(new Path(tempDir.toUri()), "second-batch-failure");
        Path firstPartition = new Path(tablePath, "part=p0");
        Path secondPartition = new Path(tablePath, "part=p1");
        List<Path> oldFiles = new ArrayList<>();
        oldFiles.addAll(writeOldFiles(fileIO, firstPartition, 4));
        oldFiles.addAll(writeOldFiles(fileIO, secondPartition, 1));
        IOException abortFailure = new IOException("discard failed after batch failure");
        TrackingCommitter firstCommitter =
                new TrackingCommitter(
                        new Path(firstPartition, "data-new.csv"),
                        null,
                        () -> {
                            throw abortFailure;
                        });
        TrackingCommitter secondCommitter =
                new TrackingCommitter(new Path(secondPartition, "data-new.csv"), null);
        TrackingCommitMessage firstMessage = new TrackingCommitMessage(firstCommitter, 3, 30);
        TrackingCommitMessage secondMessage = new TrackingCommitMessage(secondCommitter, 4, 40);
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);

        Throwable failure =
                catchThrowable(
                        () ->
                                dynamicOverwrite(tablePath, fileIO, partitionManager, 2)
                                        .commit(Arrays.asList(firstMessage, secondMessage)));

        assertThat(failure).isNotNull();
        assertThat(fileIO.discoveryPaths()).containsExactly(oldFiles.get(0));
        assertThat(fileIO.maxBatchSizeCalls()).isOne();
        assertThat(fileIO.batchCalls()).isEqualTo(2);
        assertThat(fileIO.batchInputs())
                .containsExactly(oldFiles.subList(0, 2), oldFiles.subList(2, 4));
        assertThat(fileIO.singleDeleteCalls()).isZero();
        assertThat(fileIO.exists(oldFiles.get(0))).isFalse();
        assertThat(fileIO.exists(oldFiles.get(1))).isFalse();
        assertThat(fileIO.exists(oldFiles.get(2))).isTrue();
        assertThat(fileIO.exists(oldFiles.get(3))).isTrue();
        assertThat(fileIO.exists(oldFiles.get(4))).isTrue();
        assertThat(firstCommitter.commitCalls()).isZero();
        assertThat(secondCommitter.commitCalls()).isZero();
        assertThat(firstCommitter.cleanCalls()).isZero();
        assertThat(secondCommitter.cleanCalls()).isZero();
        assertThat(firstMessage.statisticsAccessCalls()).isZero();
        assertThat(secondMessage.statisticsAccessCalls()).isZero();
        verify(partitionManager, never())
                .createPartitions(anyList(), eq(true), any(), anyBoolean());
        assertThat(firstCommitter.discardCalls()).isOne();
        assertThat(secondCommitter.discardCalls()).isOne();
        assertThat(failure.getCause()).isSameAs(fileIO.cleanupFailure());
        assertThat(failureTree(failure)).contains(fileIO.cleanupFailure(), abortFailure);
        assertThat(fileIO.cleanupFailure().getSuppressed())
                .singleElement()
                .satisfies(
                        suppressed -> assertThat(failureTree(suppressed)).contains(abortFailure));
    }

    @Test
    void testPendingInterruptAfterBatchStopsRefillAndRestoresCallerFlag() throws Exception {
        InterruptingBatchFileIO fileIO = new InterruptingBatchFileIO();
        Path tablePath = new Path(new Path(tempDir.toUri()), "interrupt");
        Path partitionPath = new Path(tablePath, "part=p");
        List<Path> oldFiles = writeOldFiles(fileIO, partitionPath, 2);
        TrackingCommitter committer =
                new TrackingCommitter(new Path(partitionPath, "data-new.csv"), null);
        FormatTableCommit commit =
                dynamicOverwrite(tablePath, fileIO, mock(FormatTablePartitionManager.class), 2);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean interruptRestored = new AtomicBoolean();
        CountDownLatch callerReturned = new CountDownLatch(1);
        Thread caller =
                new Thread(
                        () -> {
                            try {
                                commit.commit(
                                        Collections.singletonList(
                                                new TwoPhaseCommitMessage(committer)));
                            } catch (Throwable t) {
                                failure.set(t);
                            } finally {
                                interruptRestored.set(Thread.currentThread().isInterrupted());
                                callerReturned.countDown();
                            }
                        },
                        "format-batch-delete-pending-interrupt");

        caller.start();
        try {
            assertThat(callerReturned.await(10, TimeUnit.SECONDS)).isTrue();
        } finally {
            caller.interrupt();
            caller.join(TimeUnit.SECONDS.toMillis(10));
        }

        assertThat(caller.isAlive()).isFalse();
        assertThat(failure.get()).isNotNull();
        assertThat(failureTree(failure.get())).anyMatch(InterruptedException.class::isInstance);
        assertThat(interruptRestored).isTrue();
        assertThat(fileIO.batchCalls()).isOne();
        assertThat(fileIO.singleDeleteCalls()).isZero();
        assertThat(fileIO.exists(oldFiles.get(0))).isFalse();
        assertThat(fileIO.exists(oldFiles.get(1))).isTrue();
        assertThat(committer.commitCalls()).isZero();
        assertThat(committer.cleanCalls()).isZero();
        assertThat(committer.discardCalls()).isOne();
    }

    private void runExcludedMode(
            ExcludedMode mode,
            Path tablePath,
            RejectingDiscoveryFileIO fileIO,
            FormatTablePartitionManager partitionManager,
            TrackingCommitter committer) {
        Map<String, String> options = options(64, true);
        switch (mode) {
            case APPEND:
                ((FormatTableCommit)
                                table(
                                                tablePath,
                                                fileIO,
                                                partitionManager,
                                                Collections.singletonList("part"),
                                                options)
                                        .newBatchWriteBuilder()
                                        .newCommit())
                        .commit(Collections.singletonList(new TwoPhaseCommitMessage(committer)));
                return;
            case STATIC_PARTITION:
                overwriteCommit(
                                tablePath,
                                fileIO,
                                partitionManager,
                                Collections.singletonList("part"),
                                options,
                                Collections.singletonMap("part", "p"))
                        .commit(Collections.emptyList());
                return;
            case STATIC_PREFIX:
                overwriteCommit(
                                tablePath,
                                fileIO,
                                partitionManager,
                                Arrays.asList("year", "month"),
                                options,
                                Collections.singletonMap("year", "2025"))
                        .commit(Collections.emptyList());
                return;
            case WHOLE_TABLE:
                when(partitionManager.listPartitions(Collections.emptyMap(), null))
                        .thenReturn(Collections.singletonList(partition("p")));
                overwriteCommit(
                                tablePath,
                                fileIO,
                                partitionManager,
                                Collections.singletonList("part"),
                                options(64, false),
                                null)
                        .commit(Collections.emptyList());
                return;
            case TRUNCATE:
                when(partitionManager.listPartitionsByNames(anyList()))
                        .thenReturn(Collections.singletonList(partition("p")));
                ((FormatTableCommit)
                                table(
                                                tablePath,
                                                fileIO,
                                                partitionManager,
                                                Collections.singletonList("part"),
                                                options)
                                        .newBatchWriteBuilder()
                                        .newCommit())
                        .truncatePartitions(
                                Collections.singletonList(Collections.singletonMap("part", "p")));
                return;
            case TRUNCATE_TABLE:
                when(partitionManager.listPartitions(Collections.emptyMap(), null))
                        .thenReturn(Collections.singletonList(partition("p")));
                ((FormatTableCommit)
                                table(
                                                tablePath,
                                                fileIO,
                                                partitionManager,
                                                Collections.singletonList("part"),
                                                options)
                                        .newBatchWriteBuilder()
                                        .newCommit())
                        .truncateTable();
                return;
            case FILESYSTEM_DISCOVERED:
                overwriteCommit(
                                tablePath,
                                fileIO,
                                null,
                                Collections.singletonList("part"),
                                options,
                                null)
                        .commit(Collections.singletonList(new TwoPhaseCommitMessage(committer)));
                return;
            case UNPARTITIONED:
                overwriteCommit(
                                tablePath,
                                fileIO,
                                partitionManager,
                                Collections.emptyList(),
                                options,
                                null)
                        .commit(Collections.singletonList(new TwoPhaseCommitMessage(committer)));
                return;
            case LEGACY_CONSTRUCTOR:
                new FormatTableCommit(
                                tablePath.toString(),
                                Collections.singletonList("part"),
                                fileIO,
                                false,
                                PARTITION_DEFAULT_NAME.defaultValue(),
                                true,
                                TABLE,
                                null,
                                null,
                                null,
                                partitionManager,
                                true)
                        .commit(Collections.singletonList(new TwoPhaseCommitMessage(committer)));
                return;
            case SINGLE_CLEANUP_THREAD:
                overwriteCommit(
                                tablePath,
                                fileIO,
                                partitionManager,
                                Collections.singletonList("part"),
                                options(1, true),
                                null)
                        .commit(Collections.singletonList(new TwoPhaseCommitMessage(committer)));
                return;
            default:
                throw new AssertionError("Unknown mode " + mode);
        }
    }

    private FormatTableCommit dynamicOverwrite(
            Path tablePath,
            FileIO fileIO,
            FormatTablePartitionManager partitionManager,
            int cleanupThreadNum) {
        return overwriteCommit(
                tablePath,
                fileIO,
                partitionManager,
                Collections.singletonList("part"),
                options(cleanupThreadNum, true),
                null);
    }

    private FormatTableCommit overwriteCommit(
            Path tablePath,
            FileIO fileIO,
            @Nullable FormatTablePartitionManager partitionManager,
            List<String> partitionKeys,
            Map<String, String> options,
            @Nullable Map<String, String> staticPartition) {
        BatchWriteBuilder writeBuilder =
                table(tablePath, fileIO, partitionManager, partitionKeys, options)
                        .newBatchWriteBuilder();
        writeBuilder.withOverwrite(staticPartition);
        return (FormatTableCommit) writeBuilder.newCommit();
    }

    private FormatTable table(
            Path tablePath,
            FileIO fileIO,
            @Nullable FormatTablePartitionManager partitionManager,
            List<String> partitionKeys,
            Map<String, String> options) {
        RowType.Builder rowType = RowType.builder();
        for (String partitionKey : partitionKeys) {
            rowType.field(partitionKey, DataTypes.STRING());
        }
        rowType.field("id", DataTypes.INT());
        return FormatTable.builder()
                .fileIO(fileIO)
                .identifier(TABLE)
                .rowType(rowType.build())
                .partitionKeys(partitionKeys)
                .location(tablePath.toString())
                .format(FormatTable.Format.CSV)
                .options(options)
                .partitionManager(partitionManager)
                .build();
    }

    private static Map<String, String> options(int cleanupThreadNum, boolean dynamicOverwrite) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put(
                CoreOptions.FORMAT_TABLE_COMMIT_CLEANUP_THREAD_NUM.key(),
                Integer.toString(cleanupThreadNum));
        options.put(
                CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key(), Boolean.toString(dynamicOverwrite));
        return options;
    }

    private static Partition partition(String value) {
        return new Partition(Collections.singletonMap("part", value), 0, 0, 0, 0, -1, false);
    }

    private static List<Path> writeOldFiles(SortedLocalFileIO fileIO, Path partitionPath, int count)
            throws IOException {
        List<Path> files = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Path file = new Path(partitionPath, String.format("data-%05d.csv", i));
            fileIO.writeFile(file, "old", false);
            files.add(file);
        }
        return files;
    }

    private static List<List<Path>> chunks(List<Path> paths, int size) {
        List<List<Path>> chunks = new ArrayList<>();
        for (int start = 0; start < paths.size(); start += size) {
            chunks.add(new ArrayList<>(paths.subList(start, Math.min(start + size, paths.size()))));
        }
        return chunks;
    }

    private enum ExcludedMode {
        APPEND,
        STATIC_PARTITION,
        STATIC_PREFIX,
        WHOLE_TABLE,
        TRUNCATE,
        TRUNCATE_TABLE,
        FILESYSTEM_DISCOVERED,
        UNPARTITIONED,
        LEGACY_CONSTRUCTOR,
        SINGLE_CLEANUP_THREAD
    }

    private enum DiscoveryFailure {
        THROW_IO_EXCEPTION,
        RETURN_NULL_OPTIONAL
    }

    private enum MaxBatchSizeFailure {
        THROW_RUNTIME_EXCEPTION,
        RETURN_ZERO,
        RETURN_NEGATIVE
    }

    private enum BatchOutcome {
        PARTIAL_DELETE_THEN_THROW,
        FILE_NOT_FOUND_EXCEPTION,
        NULL_RESULT,
        NULL_RESULT_LIST,
        RESULT_ACCESS_THROWS,
        MISSING_PATH,
        REVERSED_PATHS,
        EXTRA_PATH,
        DUPLICATE_PATH
    }

    private interface CheckedAction {
        void run() throws IOException;
    }

    private static final class TrackingCommitter implements TwoPhaseOutputStream.Committer {

        private static final long serialVersionUID = 1L;

        private final Path targetPath;
        @Nullable private final CheckedAction commitAction;
        @Nullable private final CheckedAction discardAction;
        private final AtomicInteger commitCalls = new AtomicInteger();
        private final AtomicInteger discardCalls = new AtomicInteger();
        private final AtomicInteger cleanCalls = new AtomicInteger();

        private TrackingCommitter(Path targetPath, @Nullable CheckedAction commitAction) {
            this(targetPath, commitAction, null);
        }

        private TrackingCommitter(
                Path targetPath,
                @Nullable CheckedAction commitAction,
                @Nullable CheckedAction discardAction) {
            this.targetPath = targetPath;
            this.commitAction = commitAction;
            this.discardAction = discardAction;
        }

        @Override
        public void commit(FileIO fileIO) throws IOException {
            commitCalls.incrementAndGet();
            if (commitAction != null) {
                commitAction.run();
            }
        }

        @Override
        public void discard(FileIO fileIO) throws IOException {
            discardCalls.incrementAndGet();
            if (discardAction != null) {
                discardAction.run();
            }
        }

        @Override
        public Path targetPath() {
            return targetPath;
        }

        @Override
        public void clean(FileIO fileIO) {
            cleanCalls.incrementAndGet();
        }

        private int commitCalls() {
            return commitCalls.get();
        }

        private int discardCalls() {
            return discardCalls.get();
        }

        private int cleanCalls() {
            return cleanCalls.get();
        }
    }

    private static final class TrackingCommitMessage extends TwoPhaseCommitMessage {

        private static final long serialVersionUID = 1L;

        private final AtomicInteger statisticsAccessCalls = new AtomicInteger();

        private TrackingCommitMessage(
                TwoPhaseOutputStream.Committer committer, long recordCount, long fileSizeInBytes) {
            super(committer, recordCount, fileSizeInBytes);
        }

        @Override
        public long recordCount() {
            statisticsAccessCalls.incrementAndGet();
            return super.recordCount();
        }

        @Override
        public long fileSizeInBytes() {
            statisticsAccessCalls.incrementAndGet();
            return super.fileSizeInBytes();
        }

        private int statisticsAccessCalls() {
            return statisticsAccessCalls.get();
        }
    }

    private static final class CountingExecutorService extends AbstractExecutorService {

        private final ExecutorService delegate;
        private final AtomicInteger acceptedTasks = new AtomicInteger();

        private CountingExecutorService(ExecutorService delegate) {
            this.delegate = delegate;
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
        }

        @Override
        public List<Runnable> shutdownNow() {
            return delegate.shutdownNow();
        }

        @Override
        public boolean isShutdown() {
            return delegate.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return delegate.isTerminated();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return delegate.awaitTermination(timeout, unit);
        }

        @Override
        public void execute(Runnable command) {
            acceptedTasks.incrementAndGet();
            delegate.execute(command);
        }

        private int acceptedTasks() {
            return acceptedTasks.get();
        }
    }

    private abstract static class SortedLocalFileIO extends LocalFileIO {

        private static final long serialVersionUID = 1L;

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            FileStatus[] statuses = super.listStatus(path);
            Arrays.sort(statuses, Comparator.comparing(status -> status.getPath().toString()));
            return statuses;
        }
    }

    private abstract static class StrictBatchFileIO extends SortedLocalFileIO {

        private static final long serialVersionUID = 1L;

        private final AtomicInteger discoveryCalls = new AtomicInteger();
        private final AtomicInteger maxBatchSizeCalls = new AtomicInteger();
        private final AtomicInteger batchCalls = new AtomicInteger();
        private final AtomicInteger singleDeleteCalls = new AtomicInteger();
        private final ConcurrentLinkedQueue<Path> discoveryPaths = new ConcurrentLinkedQueue<>();
        private final ConcurrentLinkedQueue<Boolean> pathExistedAtDiscovery =
                new ConcurrentLinkedQueue<>();

        final void recordDiscovery(Path path) throws IOException {
            discoveryCalls.incrementAndGet();
            discoveryPaths.add(path);
            pathExistedAtDiscovery.add(exists(path));
        }

        final int recordMaxBatchSizeCall() {
            return maxBatchSizeCalls.incrementAndGet();
        }

        final int recordBatchCall() {
            return batchCalls.incrementAndGet();
        }

        boolean deleteInBatch(Path path) throws IOException {
            return super.delete(path, false);
        }

        @Override
        public boolean delete(Path path, boolean recursive) {
            singleDeleteCalls.incrementAndGet();
            throw new AssertionError("Strict batch mode attempted single delete for " + path);
        }

        final int discoveryCalls() {
            return discoveryCalls.get();
        }

        final int maxBatchSizeCalls() {
            return maxBatchSizeCalls.get();
        }

        final int batchCalls() {
            return batchCalls.get();
        }

        final int singleDeleteCalls() {
            return singleDeleteCalls.get();
        }

        final List<Path> discoveryPaths() {
            return new ArrayList<>(discoveryPaths);
        }

        final List<Boolean> pathExistedAtDiscovery() {
            return new ArrayList<>(pathExistedAtDiscovery);
        }
    }

    private static final class SuccessfulBatchFileIO extends StrictBatchFileIO {

        private static final long serialVersionUID = 1L;

        private final CountDownLatch finalBatchStartedOrCommitReturned;
        private final CountDownLatch releaseFinalBatch = new CountDownLatch(1);
        private final AtomicBoolean finalBatchStarted = new AtomicBoolean();
        private final AtomicInteger activeBatchCalls = new AtomicInteger();
        private final AtomicInteger maxConcurrentBatchCalls = new AtomicInteger();
        private final List<List<Path>> batchInputs =
                Collections.synchronizedList(new ArrayList<>());
        private final Map<Path, List<Path>> filesByPartition = new HashMap<>();
        private final Set<Path> existingFiles = new HashSet<>();

        private SuccessfulBatchFileIO(CountDownLatch finalBatchStartedOrCommitReturned) {
            this.finalBatchStartedOrCommitReturned = finalBatchStartedOrCommitReturned;
        }

        private List<Path> addOldFiles(Path partition, int count) {
            List<Path> files = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                Path file = new Path(partition, String.format("data-%05d.csv", i));
                files.add(file);
                existingFiles.add(file);
            }
            filesByPartition.put(partition, files);
            return new ArrayList<>(files);
        }

        @Override
        public boolean exists(Path path) {
            return filesByPartition.containsKey(path) || existingFiles.contains(path);
        }

        @Override
        public FileStatus[] listStatus(Path path) {
            List<Path> files = filesByPartition.get(path);
            if (files == null) {
                return new FileStatus[0];
            }
            List<FileStatus> statuses = new ArrayList<>();
            for (Path file : files) {
                if (existingFiles.contains(file)) {
                    statuses.add(new SyntheticFileStatus(file));
                }
            }
            return statuses.toArray(new FileStatus[0]);
        }

        @Override
        boolean deleteInBatch(Path path) {
            return existingFiles.remove(path);
        }

        @Override
        public Optional<BatchFileDeleter> batchFileDeleter(Path path) throws IOException {
            recordDiscovery(path);
            return Optional.of(
                    new BatchFileDeleter() {
                        @Override
                        public int maxBatchSize() {
                            recordMaxBatchSizeCall();
                            return OSS_BATCH_SIZE;
                        }

                        @Override
                        public BatchDeleteResult delete(List<Path> files) throws IOException {
                            int active = activeBatchCalls.incrementAndGet();
                            maxConcurrentBatchCalls.accumulateAndGet(active, Math::max);
                            try {
                                int call = recordBatchCall();
                                assertImmutable(files);
                                batchInputs.add(files);
                                if (call == 10) {
                                    finalBatchStarted.set(true);
                                    finalBatchStartedOrCommitReturned.countDown();
                                    await(releaseFinalBatch, "final batch release");
                                }
                                for (Path file : files) {
                                    if (!deleteInBatch(file)) {
                                        throw new IOException("Provider did not delete " + file);
                                    }
                                }
                                List<Path> equalButDistinctPaths = new ArrayList<>(files.size());
                                for (Path file : files) {
                                    equalButDistinctPaths.add(new Path(file.toString()));
                                }
                                return new BatchDeleteResult(equalButDistinctPaths);
                            } finally {
                                activeBatchCalls.decrementAndGet();
                            }
                        }
                    });
        }

        private static void assertImmutable(List<Path> files) {
            try {
                files.set(0, files.get(0));
                throw new AssertionError("Consumer passed a mutable batch list");
            } catch (UnsupportedOperationException expected) {
                // Required: providers may retain a chunk while validating the request.
            }
        }

        private void signalCommitReturned() {
            finalBatchStartedOrCommitReturned.countDown();
        }

        private boolean finalBatchStarted() {
            return finalBatchStarted.get();
        }

        private void releaseFinalBatch() {
            releaseFinalBatch.countDown();
        }

        private int maxConcurrentBatchCalls() {
            return maxConcurrentBatchCalls.get();
        }

        private List<List<Path>> batchInputs() {
            synchronized (batchInputs) {
                return new ArrayList<>(batchInputs);
            }
        }
    }

    private static final class SyntheticFileStatus implements FileStatus {

        private final Path path;

        private SyntheticFileStatus(Path path) {
            this.path = path;
        }

        @Override
        public long getLen() {
            return 3;
        }

        @Override
        public boolean isDir() {
            return false;
        }

        @Override
        public Path getPath() {
            return path;
        }

        @Override
        public long getModificationTime() {
            return 0;
        }
    }

    private static final class UnsupportedBatchFileIO extends SortedLocalFileIO {

        private static final long serialVersionUID = 1L;

        private final ConcurrentLinkedQueue<Path> discoveryPaths = new ConcurrentLinkedQueue<>();
        private final ConcurrentLinkedQueue<Boolean> pathExistedAtDiscovery =
                new ConcurrentLinkedQueue<>();
        private final ConcurrentLinkedQueue<Path> singleDeletePaths = new ConcurrentLinkedQueue<>();
        private final ConcurrentLinkedQueue<Boolean> recursiveDeleteArguments =
                new ConcurrentLinkedQueue<>();
        private final AtomicInteger activeSingleDeletes = new AtomicInteger();
        private final AtomicInteger maxConcurrentSingleDeletes = new AtomicInteger();
        private final AtomicInteger partitionListings = new AtomicInteger();
        private final CountDownLatch bothSingleDeletesStarted = new CountDownLatch(2);
        private final CountDownLatch releaseSingleDeletes = new CountDownLatch(1);
        @Nullable private Path partitionWhichMustNotBeRelisted;

        private void rejectRelisting(Path partitionPath) {
            partitionWhichMustNotBeRelisted = partitionPath;
        }

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            if (path.equals(partitionWhichMustNotBeRelisted)
                    && partitionListings.incrementAndGet() > 1) {
                throw new IOException("Unsupported batch fallback relisted " + path);
            }
            return super.listStatus(path);
        }

        @Override
        public Optional<BatchFileDeleter> batchFileDeleter(Path path) throws IOException {
            discoveryPaths.add(path);
            pathExistedAtDiscovery.add(super.exists(path));
            return Optional.empty();
        }

        @Override
        public boolean delete(Path path, boolean recursive) throws IOException {
            singleDeletePaths.add(path);
            recursiveDeleteArguments.add(recursive);
            int active = activeSingleDeletes.incrementAndGet();
            maxConcurrentSingleDeletes.accumulateAndGet(active, Math::max);
            bothSingleDeletesStarted.countDown();
            try {
                await(releaseSingleDeletes, "unsupported fallback single deletes to be released");
                return super.delete(path, recursive);
            } finally {
                activeSingleDeletes.decrementAndGet();
            }
        }

        private List<Path> discoveryPaths() {
            return new ArrayList<>(discoveryPaths);
        }

        private List<Boolean> pathExistedAtDiscovery() {
            return new ArrayList<>(pathExistedAtDiscovery);
        }

        private List<Path> singleDeletePaths() {
            return new ArrayList<>(singleDeletePaths);
        }

        private List<Boolean> recursiveDeleteArguments() {
            return new ArrayList<>(recursiveDeleteArguments);
        }

        private int activeSingleDeletes() {
            return activeSingleDeletes.get();
        }

        private int maxConcurrentSingleDeletes() {
            return maxConcurrentSingleDeletes.get();
        }

        private int partitionListings() {
            return partitionListings.get();
        }

        private void awaitBothSingleDeletesStarted() throws IOException {
            await(bothSingleDeletesStarted, "both unsupported fallback single deletes to start");
        }

        private void releaseSingleDeletes() {
            releaseSingleDeletes.countDown();
        }
    }

    private static final class RejectingDiscoveryFileIO extends SortedLocalFileIO {

        private static final long serialVersionUID = 1L;

        private final AtomicInteger discoveryCalls = new AtomicInteger();
        private final AtomicInteger singleDeleteCalls = new AtomicInteger();

        @Override
        public Optional<BatchFileDeleter> batchFileDeleter(Path path) {
            discoveryCalls.incrementAndGet();
            throw new AssertionError("Excluded cleanup discovered batch capability for " + path);
        }

        @Override
        public boolean delete(Path path, boolean recursive) throws IOException {
            singleDeleteCalls.incrementAndGet();
            return super.delete(path, recursive);
        }

        private int discoveryCalls() {
            return discoveryCalls.get();
        }

        private int singleDeleteCalls() {
            return singleDeleteCalls.get();
        }
    }

    private static final class DiscoveryFailureFileIO extends StrictBatchFileIO {

        private static final long serialVersionUID = 1L;

        private final DiscoveryFailure outcome;

        private DiscoveryFailureFileIO(DiscoveryFailure outcome) {
            this.outcome = outcome;
        }

        @Override
        public Optional<BatchFileDeleter> batchFileDeleter(Path path) throws IOException {
            recordDiscovery(path);
            if (outcome == DiscoveryFailure.THROW_IO_EXCEPTION) {
                throw new IOException("capability discovery failed");
            }
            return null;
        }
    }

    private static final class MaxBatchSizeFailureFileIO extends StrictBatchFileIO {

        private static final long serialVersionUID = 1L;

        private final MaxBatchSizeFailure outcome;

        private MaxBatchSizeFailureFileIO(MaxBatchSizeFailure outcome) {
            this.outcome = outcome;
        }

        @Override
        public Optional<BatchFileDeleter> batchFileDeleter(Path path) throws IOException {
            recordDiscovery(path);
            return Optional.of(
                    new BatchFileDeleter() {
                        @Override
                        public int maxBatchSize() {
                            recordMaxBatchSizeCall();
                            if (outcome == MaxBatchSizeFailure.THROW_RUNTIME_EXCEPTION) {
                                throw new IllegalStateException("provider size failed");
                            }
                            return outcome == MaxBatchSizeFailure.RETURN_ZERO ? 0 : -1;
                        }

                        @Override
                        public BatchDeleteResult delete(List<Path> files) {
                            recordBatchCall();
                            throw new AssertionError("Delete called after invalid max batch size");
                        }
                    });
        }
    }

    private static final class MaximumBatchSizeFileIO extends StrictBatchFileIO {

        private static final long serialVersionUID = 1L;

        @Override
        public Optional<BatchFileDeleter> batchFileDeleter(Path path) throws IOException {
            recordDiscovery(path);
            return Optional.of(
                    new BatchFileDeleter() {
                        @Override
                        public int maxBatchSize() {
                            recordMaxBatchSizeCall();
                            return Integer.MAX_VALUE;
                        }

                        @Override
                        public BatchDeleteResult delete(List<Path> files) throws IOException {
                            recordBatchCall();
                            List<Path> confirmed = new ArrayList<>(files.size());
                            for (Path file : files) {
                                if (!deleteInBatch(file)) {
                                    throw new IOException("Provider did not delete " + file);
                                }
                                confirmed.add(new Path(file.toString()));
                            }
                            return new BatchDeleteResult(confirmed);
                        }
                    });
        }
    }

    private static final class StrictOutcomeFileIO extends StrictBatchFileIO {

        private static final long serialVersionUID = 1L;

        private final BatchOutcome outcome;
        private final List<Path> batchInputs = new ArrayList<>();

        private StrictOutcomeFileIO(BatchOutcome outcome) {
            this.outcome = outcome;
        }

        @Override
        public Optional<BatchFileDeleter> batchFileDeleter(Path path) throws IOException {
            recordDiscovery(path);
            return Optional.of(
                    new BatchFileDeleter() {
                        @Override
                        public int maxBatchSize() {
                            recordMaxBatchSizeCall();
                            return OSS_BATCH_SIZE;
                        }

                        @Override
                        public BatchDeleteResult delete(List<Path> files) throws IOException {
                            recordBatchCall();
                            batchInputs.addAll(files);
                            switch (outcome) {
                                case PARTIAL_DELETE_THEN_THROW:
                                    if (!deleteInBatch(files.get(0))) {
                                        throw new IOException("Could not create partial success");
                                    }
                                    throw new IOException("response lost after partial success");
                                case FILE_NOT_FOUND_EXCEPTION:
                                    throw new FileNotFoundException("provider batch disappeared");
                                case NULL_RESULT:
                                    return null;
                                case NULL_RESULT_LIST:
                                    BatchDeleteResult nullList = mock(BatchDeleteResult.class);
                                    when(nullList.deletedOrNotFound()).thenReturn(null);
                                    return nullList;
                                case RESULT_ACCESS_THROWS:
                                    BatchDeleteResult throwingResult =
                                            mock(BatchDeleteResult.class);
                                    when(throwingResult.deletedOrNotFound())
                                            .thenThrow(
                                                    new IllegalStateException(
                                                            "result access failed"));
                                    return throwingResult;
                                case MISSING_PATH:
                                    return new BatchDeleteResult(
                                            Collections.singletonList(files.get(0)));
                                case REVERSED_PATHS:
                                    return new BatchDeleteResult(
                                            Arrays.asList(files.get(1), files.get(0)));
                                case EXTRA_PATH:
                                    return new BatchDeleteResult(
                                            Arrays.asList(
                                                    files.get(0),
                                                    files.get(1),
                                                    new Path(
                                                            files.get(0).getParent(),
                                                            "unrequested.csv")));
                                case DUPLICATE_PATH:
                                    return new BatchDeleteResult(
                                            Arrays.asList(files.get(0), files.get(0)));
                                default:
                                    throw new AssertionError("Unknown outcome " + outcome);
                            }
                        }
                    });
        }

        private List<Path> batchInputs() {
            return new ArrayList<>(batchInputs);
        }
    }

    private static final class SecondBatchFailureFileIO extends StrictBatchFileIO {

        private static final long serialVersionUID = 1L;

        private final IOException cleanupFailure = new IOException("batch 2 failed");
        private final List<List<Path>> batchInputs = new ArrayList<>();

        @Override
        public Optional<BatchFileDeleter> batchFileDeleter(Path path) throws IOException {
            recordDiscovery(path);
            return Optional.of(
                    new BatchFileDeleter() {
                        @Override
                        public int maxBatchSize() {
                            recordMaxBatchSizeCall();
                            return 2;
                        }

                        @Override
                        public BatchDeleteResult delete(List<Path> files) throws IOException {
                            int call = recordBatchCall();
                            batchInputs.add(new ArrayList<>(files));
                            if (call == 2) {
                                throw cleanupFailure;
                            }
                            if (call > 2) {
                                throw new AssertionError("Batch 3 started after batch 2 failed");
                            }
                            for (Path file : files) {
                                if (!deleteInBatch(file)) {
                                    throw new IOException("First batch did not delete " + file);
                                }
                            }
                            return new BatchDeleteResult(files);
                        }
                    });
        }

        private List<List<Path>> batchInputs() {
            return new ArrayList<>(batchInputs);
        }

        private IOException cleanupFailure() {
            return cleanupFailure;
        }
    }

    private static final class LaterRootListingFailureFileIO extends StrictBatchFileIO {

        private static final long serialVersionUID = 1L;

        private final Path failingRoot;

        private LaterRootListingFailureFileIO(Path failingRoot) {
            this.failingRoot = failingRoot;
        }

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            if (failingRoot.equals(path)) {
                throw new IOException("later partition listing failed");
            }
            return super.listStatus(path);
        }

        @Override
        public Optional<BatchFileDeleter> batchFileDeleter(Path path) throws IOException {
            recordDiscovery(path);
            return Optional.of(
                    new BatchFileDeleter() {
                        @Override
                        public int maxBatchSize() {
                            recordMaxBatchSizeCall();
                            return OSS_BATCH_SIZE;
                        }

                        @Override
                        public BatchDeleteResult delete(List<Path> files) {
                            recordBatchCall();
                            throw new AssertionError("Partial batch sent before listing completed");
                        }
                    });
        }
    }

    private static final class InterruptingBatchFileIO extends StrictBatchFileIO {

        private static final long serialVersionUID = 1L;

        @Override
        public Optional<BatchFileDeleter> batchFileDeleter(Path path) throws IOException {
            recordDiscovery(path);
            return Optional.of(
                    new BatchFileDeleter() {
                        @Override
                        public int maxBatchSize() {
                            recordMaxBatchSizeCall();
                            return 1;
                        }

                        @Override
                        public BatchDeleteResult delete(List<Path> files) throws IOException {
                            int call = recordBatchCall();
                            if (call != 1) {
                                throw new AssertionError(
                                        "Batch refill started despite pending interrupt");
                            }
                            if (!deleteInBatch(files.get(0))) {
                                throw new IOException("First interrupt batch was not deleted");
                            }
                            Thread.currentThread().interrupt();
                            return new BatchDeleteResult(files);
                        }
                    });
        }
    }

    private static void await(CountDownLatch latch, String description) throws IOException {
        try {
            if (!latch.await(30, TimeUnit.SECONDS)) {
                throw new IOException("Timed out waiting for " + description);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for " + description, e);
        }
    }
}
