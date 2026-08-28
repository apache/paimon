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
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.BaseMultiPartUploadCommitter;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.MultiPartUploadStore;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.RenamingTwoPhaseOutputStream;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.PartitionPathUtils;
import org.apache.paimon.utils.ReflectionUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.paimon.CoreOptions.PARTITION_DEFAULT_NAME;
import static org.apache.paimon.shade.guava30.com.google.common.base.Throwables.getCausalChain;
import static org.apache.paimon.shade.guava30.com.google.common.base.Throwables.getRootCause;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link FormatTableCommit}. */
class FormatTableCommitTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testPartitionRegistrationFailureDeletesPublishedTarget() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        Path targetPath = new Path(tablePath, "year=2025/month=10/data-1.csv");
        RenamingTwoPhaseOutputStream outputStream =
                new RenamingTwoPhaseOutputStream(fileIO, targetPath, false);
        outputStream.write(1);
        TwoPhaseOutputStream.Committer committer = outputStream.closeForCommit();
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        RuntimeException registrationFailure =
                new RuntimeException("Catalog partition registration unavailable");
        doThrow(registrationFailure).when(partitionManager).createPartitions(anyList(), eq(true));
        Identifier identifier =
                Identifier.create("catalog_partition_db", "catalog_partition_table");
        FormatTableCommit commit =
                new FormatTableCommit(
                        tablePath.toString(),
                        Arrays.asList("year", "month"),
                        fileIO,
                        false,
                        PARTITION_DEFAULT_NAME.defaultValue(),
                        false,
                        identifier,
                        null,
                        null,
                        null,
                        partitionManager,
                        /* dynamicPartitionOverwrite */ true,
                        /* cleanupThreadNum */ 1);
        TwoPhaseCommitMessage message = new TwoPhaseCommitMessage(committer);
        List<CommitMessage> messages = Collections.singletonList(message);

        assertThatThrownBy(() -> commit.commit(messages))
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("Catalog partition registration unavailable");
        assertThat(fileIO.exists(targetPath)).isFalse();
        verify(partitionManager).createPartitions(anyList(), eq(true));
        verify(partitionManager, never())
                .createPartitions(anyList(), eq(true), any(), anyBoolean());
    }

    @Test
    void testRegistrationResponseLossStillDeletesPublishedTarget() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        Path targetPath = new Path(tablePath, "part=p/data.csv");
        Identifier identifier =
                Identifier.create("catalog_partition_db", "catalog_partition_table");
        Catalog catalog = mock(Catalog.class);
        List<Map<String, String>> registeredPartitions = new ArrayList<>();
        RuntimeException registrationFailure = new RuntimeException("registration response lost");
        doAnswer(
                        invocation -> {
                            List<Map<String, String>> batch = invocation.getArgument(1);
                            registeredPartitions.addAll(batch);
                            throw registrationFailure;
                        })
                .when(catalog)
                .createPartitions(eq(identifier), anyList(), eq(true), eq(null), eq(false));
        FormatTablePartitionManager partitionManager =
                FormatTablePartitionManager.create(
                        identifier, Collections.singletonList("part"), () -> catalog);
        RenamingTwoPhaseOutputStream outputStream =
                new RenamingTwoPhaseOutputStream(fileIO, targetPath, false);
        outputStream.write(1);
        CommitMessage message = new TwoPhaseCommitMessage(outputStream.closeForCommit());
        FormatTableCommit commit =
                new FormatTableCommit(
                        tablePath.toString(),
                        Collections.singletonList("part"),
                        fileIO,
                        false,
                        PARTITION_DEFAULT_NAME.defaultValue(),
                        false,
                        identifier,
                        null,
                        null,
                        null,
                        partitionManager,
                        /* dynamicPartitionOverwrite */ true,
                        /* cleanupThreadNum */ 1);

        assertThatThrownBy(() -> commit.commit(Collections.singletonList(message)))
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("registration response lost");

        // Registration without statistics is idempotent. Even if it took effect before the
        // response was lost, deleting this attempt's unique file leaves a safe empty partition.
        assertThat(registeredPartitions).containsExactly(Collections.singletonMap("part", "p"));
        assertThat(fileIO.exists(targetPath)).isFalse();
        verify(catalog, never())
                .createPartitions(eq(identifier), anyList(), eq(true), anyList(), eq(false));
    }

    @Test
    void testHivePostRegistrationFailurePreservesOverwriteTarget() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        Path partitionPath = new Path(tablePath, "part=p");
        Path oldPath = new Path(partitionPath, "data-old.csv");
        Path targetPath = new Path(partitionPath, "data-new.csv");
        fileIO.writeFile(oldPath, "old", false);
        RenamingTwoPhaseOutputStream outputStream =
                new RenamingTwoPhaseOutputStream(fileIO, targetPath, false);
        outputStream.write(1);
        TwoPhaseOutputStream.Committer committer = outputStream.closeForCommit();
        Map<String, String> staticPartition = Collections.singletonMap("part", "p");
        FormatTableCommit commit =
                new FormatTableCommit(
                        tablePath.toString(),
                        Collections.singletonList("part"),
                        fileIO,
                        false,
                        PARTITION_DEFAULT_NAME.defaultValue(),
                        true,
                        Identifier.create("hive_db", "hive_table"),
                        staticPartition,
                        null,
                        null,
                        null,
                        /* dynamicPartitionOverwrite */ true,
                        /* cleanupThreadNum */ 1);
        PostRegistrationFailingHiveCatalog hiveCatalog =
                new PostRegistrationFailingHiveCatalog(fileIO, tablePath);
        ReflectionUtils.setPrivateFieldValue(commit, "hiveCatalog", hiveCatalog);
        List<CommitMessage> messages =
                Collections.singletonList(new TwoPhaseCommitMessage(committer));

        assertThatThrownBy(() -> commit.commit(messages))
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("Hive failed after partition registration");

        // The replacement is the only remaining copy after Hive mutates and then fails.
        assertThat(hiveCatalog.registeredPartitions).containsExactly(staticPartition);
        assertThat(fileIO.exists(oldPath)).isFalse();
        assertThat(fileIO.exists(targetPath)).isTrue();
    }

    @Test
    void testHivePostRegistrationFailureDeletesAppendTarget() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        Path targetPath = new Path(tablePath, "part=p/data-new.csv");
        RenamingTwoPhaseOutputStream outputStream =
                new RenamingTwoPhaseOutputStream(fileIO, targetPath, false);
        outputStream.write(1);
        Map<String, String> staticPartition = Collections.singletonMap("part", "p");
        FormatTableCommit commit =
                new FormatTableCommit(
                        tablePath.toString(),
                        Collections.singletonList("part"),
                        fileIO,
                        false,
                        PARTITION_DEFAULT_NAME.defaultValue(),
                        false,
                        Identifier.create("hive_db", "hive_table"),
                        staticPartition,
                        null,
                        null,
                        null,
                        /* dynamicPartitionOverwrite */ true,
                        /* cleanupThreadNum */ 1);
        PostRegistrationFailingHiveCatalog hiveCatalog =
                new PostRegistrationFailingHiveCatalog(fileIO, tablePath);
        ReflectionUtils.setPrivateFieldValue(commit, "hiveCatalog", hiveCatalog);

        assertThatThrownBy(
                        () ->
                                commit.commit(
                                        Collections.singletonList(
                                                new TwoPhaseCommitMessage(
                                                        outputStream.closeForCommit()))))
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("Hive failed after partition registration");

        // Hive registration is idempotent and carries no file statistics. Its empty partition may
        // remain, while removing this attempt's file makes a retry safe.
        assertThat(hiveCatalog.registeredPartitions).containsExactly(staticPartition);
        assertThat(fileIO.exists(targetPath)).isFalse();
    }

    @Test
    void testSuccessfulHiveAppendSurvivesAbortAfterMessageRoundTrip() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        Path targetPath = new Path(tablePath, "part=p/data-new.csv");
        RenamingTwoPhaseOutputStream outputStream =
                new RenamingTwoPhaseOutputStream(fileIO, targetPath, false);
        outputStream.write(1);
        TwoPhaseCommitMessage message = new TwoPhaseCommitMessage(outputStream.closeForCommit());
        Map<String, String> staticPartition = Collections.singletonMap("part", "p");
        FormatTableCommit commit =
                new FormatTableCommit(
                        tablePath.toString(),
                        Collections.singletonList("part"),
                        fileIO,
                        false,
                        PARTITION_DEFAULT_NAME.defaultValue(),
                        false,
                        Identifier.create("hive_db", "hive_table"),
                        staticPartition,
                        null,
                        null,
                        null,
                        /* dynamicPartitionOverwrite */ true,
                        /* cleanupThreadNum */ 1);
        RecordingHiveCatalog hiveCatalog = new RecordingHiveCatalog(fileIO, tablePath);
        ReflectionUtils.setPrivateFieldValue(commit, "hiveCatalog", hiveCatalog);

        commit.commit(Collections.singletonList(message));

        TwoPhaseCommitMessage roundTripped = InstantiationUtil.clone(message);
        FormatTableCommit abortCommit =
                new FormatTableCommit(
                        tablePath.toString(),
                        Collections.singletonList("part"),
                        fileIO,
                        false,
                        PARTITION_DEFAULT_NAME.defaultValue(),
                        false,
                        Identifier.create("hive_db", "hive_table"),
                        staticPartition,
                        null,
                        null,
                        null,
                        /* dynamicPartitionOverwrite */ true,
                        /* cleanupThreadNum */ 1);
        abortCommit.abort(Collections.singletonList(roundTripped));

        assertThat(hiveCatalog.registeredPartitions).containsExactly(staticPartition);
        assertThat(fileIO.exists(targetPath)).isTrue();
    }

    @Test
    void testFileCommitFailureStillDiscardsUncommittedFiles() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        TwoPhaseOutputStream.Committer committer = mock(TwoPhaseOutputStream.Committer.class);
        doThrow(new IOException("data commit failed")).when(committer).commit(fileIO);
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        FormatTableCommit commit =
                new FormatTableCommit(
                        tablePath.toString(),
                        Arrays.asList("year", "month"),
                        fileIO,
                        false,
                        PARTITION_DEFAULT_NAME.defaultValue(),
                        false,
                        Identifier.create("catalog_partition_db", "catalog_partition_table"),
                        null,
                        null,
                        null,
                        partitionManager,
                        /* dynamicPartitionOverwrite */ true,
                        /* cleanupThreadNum */ 1);
        CommitMessage message = new TwoPhaseCommitMessage(committer);

        assertThatThrownBy(() -> commit.commit(Collections.singletonList(message)))
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("data commit failed");

        verify(committer).discard(fileIO);
        verify(partitionManager, never())
                .createPartitions(anyList(), eq(true), any(), anyBoolean());
    }

    @Test
    void testStagingCleanupFailureDeletesPublishedFileBeforeMetadataMutation() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        Path targetPath = new Path(tablePath, "part=p/data.csv");
        TwoPhaseOutputStream.Committer committer = mock(TwoPhaseOutputStream.Committer.class);
        when(committer.targetPath()).thenReturn(targetPath);
        doAnswer(
                        ignored -> {
                            fileIO.writeFile(targetPath, "published", false);
                            return null;
                        })
                .when(committer)
                .commit(fileIO);
        doThrow(new IOException("staging cleanup failed")).when(committer).clean(fileIO);
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        FormatTableCommit commit =
                new FormatTableCommit(
                        tablePath.toString(),
                        Collections.singletonList("part"),
                        fileIO,
                        false,
                        PARTITION_DEFAULT_NAME.defaultValue(),
                        false,
                        Identifier.create("catalog_partition_db", "catalog_partition_table"),
                        null,
                        null,
                        null,
                        partitionManager,
                        /* dynamicPartitionOverwrite */ true,
                        /* cleanupThreadNum */ 1);

        assertThatThrownBy(
                        () ->
                                commit.commit(
                                        Collections.singletonList(
                                                new TwoPhaseCommitMessage(committer))))
                .hasRootCauseMessage("staging cleanup failed");

        verify(committer).discard(fileIO);
        verify(partitionManager, never())
                .createPartitions(anyList(), eq(true), any(), anyBoolean());
        assertThat(fileIO.exists(targetPath)).isFalse();
    }

    @Test
    void testOverwriteMultipartCompletionResponseLossPreservesReplacementAndReportsAbortFailure()
            throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(new Path(tempDir.toUri()), "multipart-response-loss");
        Path partitionPath = new Path(tablePath, "part=p");
        Path oldPath = new Path(partitionPath, "data-old.csv");
        Path targetPath = new Path(partitionPath, "data-new.csv");
        Path stagingPath =
                new Path(new Path(tempDir.toUri()), "multipart-staging/upload-in-progress");
        fileIO.writeFile(oldPath, "old", false);
        fileIO.writeFile(stagingPath, "staged", false);

        @SuppressWarnings("unchecked")
        MultiPartUploadStore<String, String> uploadStore = mock(MultiPartUploadStore.class);
        doAnswer(
                        invocation -> {
                            fileIO.writeFile(targetPath, "replacement", false);
                            throw new IOException("multipart completion response lost");
                        })
                .when(uploadStore)
                .completeMultipartUpload(
                        eq("part=p/data-new.csv"),
                        eq("upload-id"),
                        eq(Collections.singletonList("etag")),
                        eq(1L));
        doAnswer(
                        invocation -> {
                            fileIO.delete(stagingPath, false);
                            throw new IOException("multipart abort response lost");
                        })
                .when(uploadStore)
                .abortMultipartUpload(eq("part=p/data-new.csv"), eq("upload-id"));
        TwoPhaseOutputStream.Committer committer =
                new BaseMultiPartUploadCommitter<String, String>(
                        "upload-id",
                        Collections.singletonList("etag"),
                        "part=p/data-new.csv",
                        1L,
                        targetPath) {
                    @Override
                    protected MultiPartUploadStore<String, String> multiPartUploadStore(
                            FileIO ignored, Path ignoredTarget) {
                        return uploadStore;
                    }
                };

        Throwable failure =
                catchThrowable(
                        () ->
                                staticPartitionOverwriteCommit(tablePath, fileIO, 1)
                                        .commit(
                                                Collections.singletonList(
                                                        new TwoPhaseCommitMessage(committer))));

        assertThat(getRootCause(failure)).hasMessage("multipart completion response lost");
        assertThat(failureTree(failure))
                .extracting(Throwable::getMessage)
                .contains(
                        "Failed to discard multipart upload with ID: upload-id",
                        "multipart abort response lost");
        assertThat(fileIO.exists(oldPath)).isFalse();
        assertThat(fileIO.exists(targetPath)).isTrue();
        assertThat(fileIO.exists(stagingPath)).isFalse();
    }

    @Test
    void testOverwritePostPublishCleanFailurePreservesReplacementAndRetriesStagingCleanup()
            throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(new Path(tempDir.toUri()), "post-publish-clean-failure");
        Path partitionPath = new Path(tablePath, "part=p");
        Path oldPath = new Path(partitionPath, "data-old.csv");
        Path targetPath = new Path(partitionPath, "data-new.csv");
        Path stagingPath = new Path(new Path(tempDir.toUri()), "clean-failure-staging/data.tmp");
        fileIO.writeFile(oldPath, "old", false);
        fileIO.writeFile(stagingPath, "staged", false);
        AtomicBoolean failFirstClean = new AtomicBoolean(true);
        StagedFileCommitter committer =
                new StagedFileCommitter(targetPath, stagingPath) {
                    @Override
                    public void commit(FileIO committingFileIO) throws IOException {
                        publish(committingFileIO);
                    }

                    @Override
                    public void clean(FileIO cleaningFileIO) throws IOException {
                        if (failFirstClean.compareAndSet(true, false)) {
                            throw new IOException("staging cleanup failed");
                        }
                        super.clean(cleaningFileIO);
                    }
                };

        assertThatThrownBy(
                        () ->
                                staticPartitionOverwriteCommit(tablePath, fileIO, 1)
                                        .commit(
                                                Collections.singletonList(
                                                        new TwoPhaseCommitMessage(committer))))
                .hasRootCauseMessage("staging cleanup failed");

        assertThat(fileIO.exists(oldPath)).isFalse();
        assertThat(fileIO.exists(targetPath)).isTrue();
        assertThat(fileIO.exists(stagingPath)).isFalse();
    }

    @Test
    void testAbortAttemptsEveryRollbackAndReportsDeleteFailure() throws Exception {
        Path tablePath = new Path(tempDir.toUri());
        Path refusedPath = new Path(tablePath, "part=p/data-refused.csv");
        Path removablePath = new Path(tablePath, "part=p/data-removable.csv");
        SelectiveRefusingDeleteFileIO fileIO = new SelectiveRefusingDeleteFileIO(refusedPath);
        fileIO.writeFile(refusedPath, "published", false);
        fileIO.writeFile(removablePath, "published", false);

        TwoPhaseOutputStream.Committer first = mock(TwoPhaseOutputStream.Committer.class);
        when(first.targetPath()).thenReturn(refusedPath);
        doThrow(new IOException("discard failed")).when(first).discard(fileIO);
        TwoPhaseOutputStream.Committer second = mock(TwoPhaseOutputStream.Committer.class);
        when(second.targetPath()).thenReturn(removablePath);
        List<CommitMessage> messages =
                Arrays.asList(new TwoPhaseCommitMessage(first), new TwoPhaseCommitMessage(second));
        FormatTableCommit commit =
                new FormatTableCommit(
                        tablePath.toString(),
                        Collections.singletonList("part"),
                        fileIO,
                        false,
                        PARTITION_DEFAULT_NAME.defaultValue(),
                        false,
                        Identifier.create("rollback_db", "rollback_table"),
                        null,
                        null,
                        null,
                        null,
                        /* dynamicPartitionOverwrite */ true,
                        /* cleanupThreadNum */ 1);

        Throwable failure = catchThrowable(() -> commit.abort(messages));

        assertThat(failure).isInstanceOf(RuntimeException.class);
        assertThat(failureTree(failure))
                .extracting(Throwable::getMessage)
                .contains(
                        "discard failed",
                        "Failed to delete published Format Table file " + refusedPath);
        verify(first).discard(fileIO);
        verify(second).discard(fileIO);
        assertThat(fileIO.exists(refusedPath)).isTrue();
        assertThat(fileIO.exists(removablePath)).isFalse();
    }

    @Test
    void testRegistersRawPartitionValuesForEscapedPath() throws Exception {
        Path tablePath = new Path(tempDir.toUri());
        LinkedHashMap<String, String> rawSpec = new LinkedHashMap<>();
        rawSpec.put("year", "2025");
        rawSpec.put("month", "a b:c");
        // The writer escapes partition values when building the directory layout.
        String partitionDir = PartitionPathUtils.generatePartitionPathUtil(rawSpec, false);
        assertThat(partitionDir).isEqualTo("year=2025/month=a b%3Ac/");

        FormatTablePartitionManager partitionManager =
                commitPartitionedFile(tablePath, false, partitionDir);

        // The catalog must receive RAW values; readers re-escape them when probing directories.
        assertThat(registeredSpec(partitionManager))
                .containsExactly(entry("year", "2025"), entry("month", "a b:c"));
    }

    @Test
    void testForeignKeyValueSegmentsInLocationDoNotLeakIntoSpec() throws Exception {
        Path tablePath = new Path(new Path(tempDir.toUri()), "env=prod/warehouse/tbl");

        FormatTablePartitionManager partitionManager =
                commitPartitionedFile(tablePath, false, "year=2025/month=10");

        assertThat(registeredSpec(partitionManager))
                .containsExactly(entry("year", "2025"), entry("month", "10"));
    }

    @Test
    void testValueOnlyPathUnderForeignKeyValueSegmentRegistersRawValues() throws Exception {
        Path tablePath = new Path(new Path(tempDir.toUri()), "env=prod/warehouse/tbl");
        LinkedHashMap<String, String> rawSpec = new LinkedHashMap<>();
        rawSpec.put("year", "2025");
        rawSpec.put("month", "a:b");
        String partitionDir = PartitionPathUtils.generatePartitionPathUtil(rawSpec, true);
        assertThat(partitionDir).isEqualTo("2025/a%3Ab/");

        FormatTablePartitionManager partitionManager =
                commitPartitionedFile(tablePath, true, partitionDir);

        assertThat(registeredSpec(partitionManager))
                .containsExactly(entry("year", "2025"), entry("month", "a:b"));
    }

    @Test
    void testOverwriteKeepsFilesOfConcurrentWritersStagingTrees() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        Path partitionPath = new Path(tablePath, "year=2025/month=10");
        Path previousDataFile = new Path(partitionPath, "data-old.csv");
        fileIO.writeFile(previousDataFile, "1", false);
        // A concurrent job is mid-write in this partition, under a magic committer's tree. Its
        // file carries an ordinary data file name; only the directories above it say otherwise.
        Path stagingFile =
                new Path(
                        partitionPath,
                        "__magic_job-6e7f/tasks/attempt_202607271200_0001_m_000010_15"
                                + "/__base/part-00010.csv");
        fileIO.writeFile(stagingFile, "2", false);

        Path targetPath = new Path(partitionPath, "data-new.csv");
        RenamingTwoPhaseOutputStream outputStream =
                new RenamingTwoPhaseOutputStream(fileIO, targetPath, false);
        outputStream.write(1);
        TwoPhaseOutputStream.Committer committer = outputStream.closeForCommit();
        Map<String, String> staticPartition = new LinkedHashMap<>();
        staticPartition.put("year", "2025");
        staticPartition.put("month", "10");
        FormatTableCommit commit =
                new FormatTableCommit(
                        tablePath.toString(),
                        Arrays.asList("year", "month"),
                        fileIO,
                        false,
                        PARTITION_DEFAULT_NAME.defaultValue(),
                        true,
                        Identifier.create("overwrite_db", "overwrite_table"),
                        staticPartition,
                        null,
                        null,
                        null,
                        /* dynamicPartitionOverwrite */ true,
                        /* cleanupThreadNum */ 1);

        commit.commit(Collections.singletonList(new TwoPhaseCommitMessage(committer)));

        assertThat(fileIO.exists(previousDataFile)).isFalse();
        assertThat(fileIO.exists(targetPath)).isTrue();
        assertThat(fileIO.exists(stagingFile)).isTrue();
    }

    @Test
    void testOverwritingAPrefixKeepsStagingTreesSittingAtAPartitionLevel() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        Path staticPrefix = new Path(tablePath, "year=2025");
        Path staleFile = new Path(staticPrefix, "month=10/data-old.csv");
        fileIO.writeFile(staleFile, "1", false);
        // A concurrent job overwriting the same prefix with a dynamic month stages below the
        // prefix itself, so its staging root stands where the month directories do rather than
        // inside one of them. Being at a partition level does not make it partition data: the
        // month directories it holds are the job's own, to be moved into place at its commit.
        List<Path> stagingFiles =
                Arrays.asList(
                        new Path(staticPrefix, "_temporary/attempt/part.csv"),
                        new Path(
                                staticPrefix,
                                "_temporary/0/attempt_202607271200_0001_m_000012_17"
                                        + "/month=12/part-00012.csv"),
                        new Path(
                                staticPrefix,
                                "_temporary/0/_temporary/attempt_202607271200_0001_m_000011_16"
                                        + "/month=11/part-00011.csv"));
        for (Path stagingFile : stagingFiles) {
            fileIO.writeFile(stagingFile, "2", false);
        }

        // INSERT OVERWRITE ... PARTITION (year = '2025'), month left dynamic.
        FormatTableCommit commit =
                new FormatTableCommit(
                        tablePath.toString(),
                        Arrays.asList("year", "month"),
                        fileIO,
                        false,
                        PARTITION_DEFAULT_NAME.defaultValue(),
                        true,
                        Identifier.create("overwrite_db", "overwrite_table"),
                        Collections.singletonMap("year", "2025"),
                        null,
                        null,
                        null,
                        /* dynamicPartitionOverwrite */ true,
                        /* cleanupThreadNum */ 1);

        commit.commit(Collections.emptyList());

        assertThat(fileIO.exists(staleFile)).isFalse();
        for (Path stagingFile : stagingFiles) {
            assertThat(fileIO.exists(stagingFile)).isTrue();
        }
    }

    @Test
    void testOverwritingAPrefixClearsTheDefaultPartitionDirectory() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        // Value-only layout: the directory of a null partition value is the default partition
        // name, which starts with '_' without being a staging directory. The scan reads it, so an
        // overwrite has to clear it too.
        Path defaultPartition =
                new Path(tablePath, "2025/" + PARTITION_DEFAULT_NAME.defaultValue());
        Path staleFile = new Path(defaultPartition, "data-old.csv");
        fileIO.writeFile(staleFile, "1", false);
        Path staleSibling = new Path(tablePath, "2025/10/data-old.csv");
        fileIO.writeFile(staleSibling, "1", false);
        Path stagedFile = new Path(defaultPartition, "__magic_job-6e7f/__base/part-00010.csv");
        fileIO.writeFile(stagedFile, "2", false);
        // The exemption is that one directory name and no other: a staging tree standing next to
        // the partition directories is still a staging tree.
        Path stagedNextToThePartitions =
                new Path(tablePath, "2025/_temporary/attempt/part-00011.csv");
        fileIO.writeFile(stagedNextToThePartitions, "2", false);

        FormatTableCommit commit =
                new FormatTableCommit(
                        tablePath.toString(),
                        Arrays.asList("year", "month"),
                        fileIO,
                        true,
                        PARTITION_DEFAULT_NAME.defaultValue(),
                        true,
                        Identifier.create("overwrite_db", "overwrite_table"),
                        Collections.singletonMap("year", "2025"),
                        null,
                        null,
                        null,
                        /* dynamicPartitionOverwrite */ true,
                        /* cleanupThreadNum */ 1);

        commit.commit(Collections.emptyList());

        assertThat(fileIO.exists(staleFile)).isFalse();
        assertThat(fileIO.exists(staleSibling)).isFalse();
        // Inside the partition, hidden still means staging.
        assertThat(fileIO.exists(stagedFile)).isTrue();
        assertThat(fileIO.exists(stagedNextToThePartitions)).isTrue();
    }

    @Test
    void testPathNotMatchingThePartitionKeysFails() throws Exception {
        Path tablePath = new Path(tempDir.toUri());
        Path targetPath = new Path(tablePath, "year=2025/day=10/data-1.csv");

        // The message names the path and the declared keys, which is what tells a reader that
        // 'day' is not where 'month' was expected.
        assertThatThrownBy(() -> commitPartitionedFile(tablePath, false, "year=2025/day=10"))
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .rootCause()
                .hasMessageContaining("year=2025/day=10")
                .hasMessageContaining("catalog_partition_db.catalog_partition_table")
                .hasMessageContaining("[year, month]");
        assertThat(LocalFileIO.create().exists(targetPath)).isFalse();
    }

    @Test
    void testValueOnlyStaticPartitionCannotEscapeTableLocation() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path parentPath = new Path(tempDir.toUri());
        Path tablePath = new Path(parentPath, "table");
        Path siblingPath = new Path(parentPath, "keep");
        fileIO.mkdirs(tablePath);
        fileIO.mkdirs(siblingPath);
        Map<String, String> staticPartition = Collections.singletonMap("year", "..");
        FormatTableCommit commit =
                new FormatTableCommit(
                        tablePath.toString(),
                        Collections.singletonList("year"),
                        fileIO,
                        true,
                        PARTITION_DEFAULT_NAME.defaultValue(),
                        true,
                        Identifier.create("catalog_partition_db", "catalog_partition_table"),
                        staticPartition,
                        null,
                        null,
                        null,
                        /* dynamicPartitionOverwrite */ true,
                        /* cleanupThreadNum */ 1);

        assertThatThrownBy(() -> commit.commit(Collections.emptyList()))
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage(
                        "Partition value '..' cannot be used as a partition path component.");
        assertThat(fileIO.exists(tablePath)).isTrue();
        assertThat(fileIO.exists(siblingPath)).isTrue();
    }

    @Test
    void testTruncateTableEmptiesEveryPartitionAndLeavesThePartitionsThemselves() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        Path october = new Path(tablePath, "year=2025/month=10");
        Path november = new Path(tablePath, "year=2025/month=11");
        Path octoberData = new Path(october, "data-1.csv");
        Path novemberData = new Path(november, "data-2.csv");
        fileIO.writeFile(octoberData, "1", false);
        fileIO.writeFile(novemberData, "2", false);
        // Another writer is mid-write in this partition; its staging tree is not table data.
        Path stagingFile = new Path(october, "_temporary/attempt/part-00000.csv");
        fileIO.writeFile(stagingFile, "3", false);
        FormatTableCommit commit =
                truncatingCommit(tablePath, fileIO, false, null, "year", "month");

        commit.truncateTable();

        assertThat(fileIO.exists(octoberData)).isFalse();
        assertThat(fileIO.exists(novemberData)).isFalse();
        assertThat(fileIO.exists(stagingFile)).isTrue();
        // Emptying a table does not redefine which partitions it has: the directories stay.
        assertThat(fileIO.exists(october)).isTrue();
        assertThat(fileIO.exists(november)).isTrue();
    }

    @Test
    void testTruncateTableEmptiesTheRegisteredPartitionsOfACatalogManagedTable() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        Path registered = new Path(tablePath, "year=2025/month=10");
        Path registeredData = new Path(registered, "data-1.csv");
        fileIO.writeFile(registeredData, "1", false);
        // Dropped there by something outside Paimon and not registered yet, so it is not part of
        // the table: MSCK REPAIR TABLE is what would make it so.
        Path awaitingRepair = new Path(tablePath, "year=2025/month=11");
        Path awaitingRepairData = new Path(awaitingRepair, "data-2.csv");
        fileIO.writeFile(awaitingRepairData, "2", false);
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        when(partitionManager.listPartitions(Collections.emptyMap(), null))
                .thenReturn(
                        Collections.singletonList(
                                new Partition(partitionSpec("2025", "10"), 0, 0, 0, 0, -1, false)));
        FormatTableCommit commit =
                truncatingCommit(tablePath, fileIO, false, partitionManager, "year", "month");

        commit.truncateTable();

        assertThat(fileIO.exists(registeredData)).isFalse();
        assertThat(fileIO.exists(registered)).isTrue();
        assertThat(fileIO.exists(awaitingRepairData)).isTrue();
    }

    @Test
    void testTruncateTableOnlyEmptiesTheDirectoriesThatAreItsPartitions() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        Path partitionData = new Path(tablePath, "year=2025/month=10/data-1.csv");
        fileIO.writeFile(partitionData, "1", false);
        // Neither of these is a partition: the scan reads the directories that parse into the
        // partition keys, so nothing else under the table directory is table data.
        Path atTheTableRoot = new Path(tablePath, "notes.csv");
        fileIO.writeFile(atTheTableRoot, "2", false);
        Path outsideThePartitionLayout = new Path(tablePath, "tmp/unknown/x.csv");
        fileIO.writeFile(outsideThePartitionLayout, "3", false);
        FormatTableCommit commit =
                truncatingCommit(tablePath, fileIO, false, null, "year", "month");

        commit.truncateTable();

        assertThat(fileIO.exists(partitionData)).isFalse();
        assertThat(fileIO.exists(atTheTableRoot)).isTrue();
        assertThat(fileIO.exists(outsideThePartitionLayout)).isTrue();
    }

    @Test
    void testTruncateTableClearsAValueOnlyDefaultPartitionBelowTheTableDirectory()
            throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        // Value-only layout: a null month is the default partition name, which starts with '_'
        // without being a staging directory, and here it sits below the listed directory instead
        // of being it.
        Path defaultPartition =
                new Path(tablePath, "2025/" + PARTITION_DEFAULT_NAME.defaultValue());
        Path staleFile = new Path(defaultPartition, "data-old.csv");
        fileIO.writeFile(staleFile, "1", false);
        Path stagingFile = new Path(tablePath, "2025/_temporary/attempt/part-00000.csv");
        fileIO.writeFile(stagingFile, "2", false);
        FormatTableCommit commit = truncatingCommit(tablePath, fileIO, true, null, "year", "month");

        commit.truncateTable();

        assertThat(fileIO.exists(staleFile)).isFalse();
        assertThat(fileIO.exists(stagingFile)).isTrue();
        assertThat(fileIO.exists(defaultPartition)).isTrue();
    }

    @Test
    void testTruncateTableOfAnUnpartitionedTableClearsTheTableDirectory() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        Path dataFile = new Path(tablePath, "data-1.csv");
        fileIO.writeFile(dataFile, "1", false);
        Path stagingFile = new Path(tablePath, "_temporary/attempt/part-00000.csv");
        fileIO.writeFile(stagingFile, "2", false);
        FormatTableCommit commit = truncatingCommit(tablePath, fileIO, false, null);

        commit.truncateTable();

        assertThat(fileIO.exists(dataFile)).isFalse();
        assertThat(fileIO.exists(stagingFile)).isTrue();
        assertThat(fileIO.exists(tablePath)).isTrue();
    }

    @Test
    void testTruncatePartitionsStaysInsideThePartitionsItNames() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        Path october = new Path(tablePath, "year=2025/month=10");
        Path november = new Path(tablePath, "year=2025/month=11");
        Path octoberData = new Path(october, "data-1.csv");
        Path novemberData = new Path(november, "data-2.csv");
        fileIO.writeFile(octoberData, "1", false);
        fileIO.writeFile(novemberData, "2", false);
        Map<String, String> october2025 = new LinkedHashMap<>();
        october2025.put("year", "2025");
        october2025.put("month", "10");
        FormatTableCommit commit =
                truncatingCommit(tablePath, fileIO, false, null, "year", "month");

        commit.truncatePartitions(Collections.singletonList(october2025));

        assertThat(fileIO.exists(octoberData)).isFalse();
        assertThat(fileIO.exists(october)).isTrue();
        assertThat(fileIO.exists(novemberData)).isTrue();
    }

    @Test
    void testTruncatingAPrefixClearsThePartitionsBelowItButNotStagingTrees() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        Path staleFile = new Path(tablePath, "year=2025/month=10/data-old.csv");
        fileIO.writeFile(staleFile, "1", false);
        // A job writing this prefix with the month left dynamic stages exactly where the month
        // directories sit, so a directory at a partition level is not automatically partition data.
        Path stagingFile =
                new Path(tablePath, "year=2025/_temporary/attempt/month=11/part-00011.csv");
        fileIO.writeFile(stagingFile, "2", false);
        Path otherYear = new Path(tablePath, "year=2024/month=10/data-old.csv");
        fileIO.writeFile(otherYear, "3", false);
        FormatTableCommit commit =
                truncatingCommit(tablePath, fileIO, false, null, "year", "month");

        commit.truncatePartitions(
                Collections.singletonList(Collections.singletonMap("year", "2025")));

        assertThat(fileIO.exists(staleFile)).isFalse();
        assertThat(fileIO.exists(stagingFile)).isTrue();
        assertThat(fileIO.exists(otherYear)).isTrue();
    }

    @Test
    void testTruncatingTheValueOnlyDefaultPartitionClearsIt() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        // Value-only layout: a null partition value is the default partition name, which starts
        // with '_' without being a staging directory. The scan reads it, so TRUNCATE clears it.
        Path defaultPartition = new Path(tablePath, PARTITION_DEFAULT_NAME.defaultValue());
        Path staleFile = new Path(defaultPartition, "data-old.csv");
        fileIO.writeFile(staleFile, "1", false);
        FormatTableCommit commit = truncatingCommit(tablePath, fileIO, true, null, "year");

        commit.truncatePartitions(
                Collections.singletonList(
                        Collections.singletonMap("year", PARTITION_DEFAULT_NAME.defaultValue())));

        assertThat(fileIO.exists(staleFile)).isFalse();
        assertThat(fileIO.exists(defaultPartition)).isTrue();
    }

    @Test
    void testOverwritingWithoutAStaticPartitionEmptiesAnUnpartitionedTable() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        Path previousData = new Path(tablePath, "data-1.csv");
        fileIO.writeFile(previousData, "1", false);
        // Another writer is mid-write; its staging tree is not table data.
        Path stagingFile = new Path(tablePath, "_temporary/attempt/part-00000.csv");
        fileIO.writeFile(stagingFile, "2", false);
        FormatTableCommit commit = overwritingCommit(tablePath, fileIO, true);

        // Nothing to write: the query behind the statement returned no rows.
        commit.commit(Collections.emptyList());

        // An unpartitioned overwrite is about the table, so the files it replaces are the table's,
        // not the ones this commit happens to write.
        assertThat(fileIO.exists(previousData)).isFalse();
        assertThat(fileIO.exists(stagingFile)).isTrue();
    }

    @Test
    void testOverwritingAPartitionedTableFollowsDynamicPartitionOverwrite() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path dynamicTable = new Path(new Path(tempDir.toUri()), "dynamic");
        Path staticTable = new Path(new Path(tempDir.toUri()), "static");
        for (Path table : Arrays.asList(dynamicTable, staticTable)) {
            fileIO.writeFile(new Path(table, "year=2025/month=10/data-1.csv"), "1", false);
            fileIO.writeFile(new Path(table, "year=2025/month=11/data-2.csv"), "2", false);
        }

        overwritingCommit(dynamicTable, fileIO, true, "year", "month")
                .commit(Collections.emptyList());
        overwritingCommit(staticTable, fileIO, false, "year", "month")
                .commit(Collections.emptyList());

        // Dynamic overwrite selects the partitions written, and this commit wrote none.
        assertThat(fileIO.exists(new Path(dynamicTable, "year=2025/month=10/data-1.csv"))).isTrue();
        assertThat(fileIO.exists(new Path(dynamicTable, "year=2025/month=11/data-2.csv"))).isTrue();
        // With it off, the statement is about the whole table, whatever the query returned.
        assertThat(fileIO.exists(new Path(staticTable, "year=2025/month=10/data-1.csv"))).isFalse();
        assertThat(fileIO.exists(new Path(staticTable, "year=2025/month=11/data-2.csv"))).isFalse();
    }

    @Test
    void testDynamicOverwriteReplacesOnlyThePartitionsItWrites() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        Path rewritten = new Path(tablePath, "year=2025/month=10/data-1.csv");
        Path untouched = new Path(tablePath, "year=2025/month=11/data-2.csv");
        fileIO.writeFile(rewritten, "1", false);
        fileIO.writeFile(untouched, "2", false);
        RenamingTwoPhaseOutputStream outputStream =
                new RenamingTwoPhaseOutputStream(
                        fileIO, new Path(tablePath, "year=2025/month=10/data-new.csv"), false);
        outputStream.write(1);
        TwoPhaseOutputStream.Committer committer = outputStream.closeForCommit();

        overwritingCommit(tablePath, fileIO, true, "year", "month")
                .commit(Collections.singletonList(new TwoPhaseCommitMessage(committer)));

        // The partitions a dynamic overwrite replaces are the ones it writes, and only those.
        assertThat(fileIO.exists(rewritten)).isFalse();
        assertThat(fileIO.exists(new Path(tablePath, "year=2025/month=10/data-new.csv"))).isTrue();
        assertThat(fileIO.exists(untouched)).isTrue();
    }

    @Test
    void testOverwritingTheWholeTableLeavesADirectoryThatIsNoPartitionOfIt() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        fileIO.writeFile(new Path(tablePath, "year=2025/month=10/data-1.csv"), "1", false);
        // Neither of these parses into the partition keys, so no scan of the table reads them:
        // replacing what the table holds is none of their business.
        fileIO.writeFile(new Path(tablePath, "year=2025/nomonth/data-2.csv"), "2", false);
        fileIO.writeFile(new Path(tablePath, "loose.csv"), "3", false);

        overwritingCommit(tablePath, fileIO, false, "year", "month")
                .commit(Collections.emptyList());

        assertThat(fileIO.exists(new Path(tablePath, "year=2025/month=10/data-1.csv"))).isFalse();
        assertThat(fileIO.exists(new Path(tablePath, "year=2025/nomonth/data-2.csv"))).isTrue();
        assertThat(fileIO.exists(new Path(tablePath, "loose.csv"))).isTrue();
    }

    @Test
    void testCatalogManagedBuilderUses64WayCleanupByDefault() throws Exception {
        ParallelDeleteFileIO fileIO = new ParallelDeleteFileIO(64, true);
        Path tablePath = new Path(tempDir.toUri());
        Path partitionPath = new Path(tablePath, "part=p");
        writeOldFiles(fileIO, partitionPath, 65);
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        TwoPhaseOutputStream.Committer committer = mock(TwoPhaseOutputStream.Committer.class);
        when(committer.targetPath()).thenReturn(new Path(partitionPath, "data-new.csv"));
        doAnswer(
                        invocation -> {
                            assertThat(fileIO.activeDeletes()).isZero();
                            return null;
                        })
                .when(committer)
                .commit(fileIO);
        FormatTableCommit commit =
                builderOverwriteCommit(
                        tablePath,
                        fileIO,
                        partitionManager,
                        Collections.emptyMap(),
                        Collections.singletonMap("part", "p"));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> result =
                    executor.submit(
                            () ->
                                    commit.commit(
                                            Collections.singletonList(
                                                    new TwoPhaseCommitMessage(committer))));

            assertThat(fileIO.awaitFirstWave()).isTrue();
            verify(committer, never()).commit(fileIO);

            fileIO.releaseFirstWave();
            result.get(10, TimeUnit.SECONDS);
            assertThat(fileIO.deleteCalls()).isEqualTo(65);
            assertThat(fileIO.maxConcurrentDeletes()).isEqualTo(64);
            verify(committer).commit(fileIO);
        } finally {
            fileIO.releaseFirstWave();
            executor.shutdownNow();
        }
    }

    @Test
    void testCatalogManagedBuilderPublishesSamePartitionConcurrentlyAndWaitsForBarrier()
            throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        Path partitionPath = new Path(tablePath, "part=p");
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        CountDownLatch firstTwoStarted = new CountDownLatch(2);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch releaseSecond = new CountDownLatch(1);
        CountDownLatch thirdFinished = new CountDownLatch(1);
        AtomicInteger activePublishes = new AtomicInteger();
        AtomicInteger maxConcurrentPublishes = new AtomicInteger();
        List<TwoPhaseOutputStream.Committer> committers = new ArrayList<>();
        List<CommitMessage> messages = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            int index = i;
            TwoPhaseOutputStream.Committer committer = mock(TwoPhaseOutputStream.Committer.class);
            when(committer.targetPath()).thenReturn(new Path(partitionPath, "data-" + i + ".csv"));
            doAnswer(
                            invocation -> {
                                int active = activePublishes.incrementAndGet();
                                maxConcurrentPublishes.updateAndGet(
                                        previous -> Math.max(previous, active));
                                try {
                                    if (index == 0) {
                                        firstTwoStarted.countDown();
                                        if (!releaseFirst.await(10, TimeUnit.SECONDS)) {
                                            throw new IOException(
                                                    "Timed out waiting to release first publication");
                                        }
                                    } else if (index == 1) {
                                        firstTwoStarted.countDown();
                                        if (!releaseSecond.await(10, TimeUnit.SECONDS)) {
                                            throw new IOException(
                                                    "Timed out waiting to release second publication");
                                        }
                                    }
                                    return null;
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    throw new IOException("File publication was interrupted", e);
                                } finally {
                                    activePublishes.decrementAndGet();
                                    if (index == 2) {
                                        thirdFinished.countDown();
                                    }
                                }
                            })
                    .when(committer)
                    .commit(fileIO);
            committers.add(committer);
            messages.add(new TwoPhaseCommitMessage(committer));
        }
        FormatTableCommit commit =
                (FormatTableCommit)
                        formatTable(
                                        tablePath,
                                        fileIO,
                                        partitionManager,
                                        Collections.singletonMap(
                                                CoreOptions.FORMAT_TABLE_COMMIT_PUBLISH_THREAD_NUM
                                                        .key(),
                                                "2"))
                                .newBatchWriteBuilder()
                                .newCommit();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> result = executor.submit(() -> commit.commit(messages));
        try {
            assertThat(firstTwoStarted.await(3, TimeUnit.SECONDS)).isTrue();
            assertThat(activePublishes).hasValue(2);

            releaseFirst.countDown();
            assertThat(thirdFinished.await(3, TimeUnit.SECONDS)).isTrue();
            assertThat(activePublishes).hasValue(1);
            verify(partitionManager, never())
                    .createPartitions(anyList(), eq(true), any(), anyBoolean());
            for (TwoPhaseOutputStream.Committer committer : committers) {
                verify(committer, never()).clean(fileIO);
            }

            releaseSecond.countDown();
            result.get(10, TimeUnit.SECONDS);

            assertThat(maxConcurrentPublishes).hasValue(2);
            for (TwoPhaseOutputStream.Committer committer : committers) {
                verify(committer).clean(fileIO);
            }
            verify(partitionManager).createPartitions(anyList(), eq(true), any(), eq(false));
        } finally {
            releaseFirst.countDown();
            releaseSecond.countDown();
            if (!result.isDone()) {
                result.get(10, TimeUnit.SECONDS);
            }
            executor.shutdownNow();
        }
    }

    @Test
    void testPublishFailureDrainsRunningWorkBeforeAbort() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        Path partitionPath = new Path(tablePath, "part=p");
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        CountDownLatch secondStarted = new CountDownLatch(1);
        CountDownLatch releaseSecond = new CountDownLatch(1);
        AtomicInteger activePublishes = new AtomicInteger();
        ConcurrentLinkedQueue<Integer> activePublishesAtDiscard = new ConcurrentLinkedQueue<>();
        List<TwoPhaseOutputStream.Committer> committers = new ArrayList<>();
        List<Path> targetPaths = new ArrayList<>();
        List<CommitMessage> messages = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            TwoPhaseOutputStream.Committer committer = mock(TwoPhaseOutputStream.Committer.class);
            Path targetPath = new Path(partitionPath, "data-" + i + ".csv");
            when(committer.targetPath()).thenReturn(targetPath);
            doAnswer(
                            invocation -> {
                                activePublishesAtDiscard.add(activePublishes.get());
                                return null;
                            })
                    .when(committer)
                    .discard(fileIO);
            committers.add(committer);
            targetPaths.add(targetPath);
            messages.add(new TwoPhaseCommitMessage(committer));
        }
        doAnswer(
                        invocation -> {
                            activePublishes.incrementAndGet();
                            try {
                                if (!secondStarted.await(10, TimeUnit.SECONDS)) {
                                    throw new IOException("The second publication did not start");
                                }
                                fileIO.writeFile(targetPaths.get(0), "published", false);
                                throw new IOException("publish failed");
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new IOException("File publication was interrupted", e);
                            } finally {
                                activePublishes.decrementAndGet();
                            }
                        })
                .when(committers.get(0))
                .commit(fileIO);
        doAnswer(
                        invocation -> {
                            activePublishes.incrementAndGet();
                            secondStarted.countDown();
                            try {
                                if (!releaseSecond.await(10, TimeUnit.SECONDS)) {
                                    throw new IOException(
                                            "Timed out waiting to release publication");
                                }
                                fileIO.writeFile(targetPaths.get(1), "published", false);
                                return null;
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new IOException("File publication was interrupted", e);
                            } finally {
                                activePublishes.decrementAndGet();
                            }
                        })
                .when(committers.get(1))
                .commit(fileIO);
        FormatTableCommit commit =
                (FormatTableCommit)
                        formatTable(
                                        tablePath,
                                        fileIO,
                                        partitionManager,
                                        Collections.singletonMap(
                                                CoreOptions.FORMAT_TABLE_COMMIT_PUBLISH_THREAD_NUM
                                                        .key(),
                                                "2"))
                                .newBatchWriteBuilder()
                                .newCommit();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> result = executor.submit(() -> commit.commit(messages));
        try {
            assertThat(secondStarted.await(10, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> result.get(300, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            for (TwoPhaseOutputStream.Committer committer : committers) {
                verify(committer, never()).discard(fileIO);
            }

            releaseSecond.countDown();
            assertThat(getRootCause(awaitFailure(result))).hasMessage("publish failed");

            verify(committers.get(2), never()).commit(fileIO);
            assertThat(activePublishesAtDiscard).containsExactly(0, 0, 0);
            for (TwoPhaseOutputStream.Committer committer : committers) {
                verify(committer).discard(fileIO);
            }
            assertThat(fileIO.exists(targetPaths.get(0))).isFalse();
            assertThat(fileIO.exists(targetPaths.get(1))).isFalse();
        } finally {
            releaseSecond.countDown();
            if (!result.isDone()) {
                try {
                    result.get(10, TimeUnit.SECONDS);
                } catch (ExecutionException ignored) {
                    // The test expects the first publication to fail.
                }
            }
            executor.shutdownNow();
        }
    }

    @Test
    void testOverwritePartialParallelPublishFailurePreservesReplacementAndCleansStaging()
            throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(new Path(tempDir.toUri()), "partial-parallel-publish");
        Path partitionPath = new Path(tablePath, "part=p");
        Path oldPath = new Path(partitionPath, "data-old.csv");
        Path successfulTarget = new Path(partitionPath, "data-success.csv");
        Path failedTarget = new Path(partitionPath, "data-failed.csv");
        Path successfulStaging = new Path(new Path(tempDir.toUri()), "partial-staging/success.tmp");
        Path failedStaging = new Path(new Path(tempDir.toUri()), "partial-staging/failed.tmp");
        fileIO.writeFile(oldPath, "old", false);
        fileIO.writeFile(successfulStaging, "staged", false);
        fileIO.writeFile(failedStaging, "staged", false);

        CountDownLatch bothPublishesStarted = new CountDownLatch(2);
        CountDownLatch replacementPublished = new CountDownLatch(1);
        StagedFileCommitter successfulCommitter =
                new StagedFileCommitter(successfulTarget, successfulStaging) {
                    @Override
                    public void commit(FileIO committingFileIO) throws IOException {
                        bothPublishesStarted.countDown();
                        awaitLatch(bothPublishesStarted, "both overwrite publications to start");
                        publish(committingFileIO);
                        replacementPublished.countDown();
                    }
                };
        StagedFileCommitter failingCommitter =
                new StagedFileCommitter(failedTarget, failedStaging) {
                    @Override
                    public void commit(FileIO committingFileIO) throws IOException {
                        bothPublishesStarted.countDown();
                        awaitLatch(bothPublishesStarted, "both overwrite publications to start");
                        awaitLatch(replacementPublished, "the parallel replacement publication");
                        publish(committingFileIO);
                        throw new IOException("parallel publish failed");
                    }
                };
        List<CommitMessage> messages =
                Arrays.asList(
                        new TwoPhaseCommitMessage(successfulCommitter),
                        new TwoPhaseCommitMessage(failingCommitter));

        assertThatThrownBy(
                        () -> staticPartitionOverwriteCommit(tablePath, fileIO, 2).commit(messages))
                .hasRootCauseMessage("parallel publish failed");

        assertThat(fileIO.exists(oldPath)).isFalse();
        assertThat(fileIO.exists(successfulTarget)).isTrue();
        assertThat(fileIO.exists(failedTarget)).isTrue();
        assertThat(fileIO.exists(successfulStaging)).isFalse();
        assertThat(fileIO.exists(failedStaging)).isFalse();
    }

    @Test
    void testPublishConcurrencyIsGatedToCatalogManagedPartitionedTables() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        Map<String, String> options =
                Collections.singletonMap(
                        CoreOptions.FORMAT_TABLE_COMMIT_PUBLISH_THREAD_NUM.key(), "64");

        FormatTableCommit filesystemDiscovered =
                (FormatTableCommit)
                        formatTable(new Path(tablePath, "filesystem"), fileIO, null, options)
                                .newBatchWriteBuilder()
                                .newCommit();
        assertPublishesOnCaller(
                filesystemDiscovered, fileIO, new Path(tablePath, "filesystem/part=p"));

        FormatTableCommit unpartitioned =
                builderUnpartitionedOverwriteCommit(
                        new Path(tablePath, "unpartitioned"),
                        fileIO,
                        mock(FormatTablePartitionManager.class),
                        options);
        assertPublishesOnCaller(unpartitioned, fileIO, new Path(tablePath, "unpartitioned"));
    }

    @Test
    void testCatalogManagedBuilderHonorsConfiguredSerialCleanup() throws Exception {
        SerialProbeFileIO fileIO = new SerialProbeFileIO();
        Path tablePath = new Path(tempDir.toUri());
        Path partitionPath = new Path(tablePath, "part=p");
        writeOldFiles(fileIO, partitionPath, 3);
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        TwoPhaseOutputStream.Committer committer = mock(TwoPhaseOutputStream.Committer.class);
        when(committer.targetPath()).thenReturn(new Path(partitionPath, "data-new.csv"));
        FormatTableCommit commit =
                builderOverwriteCommit(
                        tablePath,
                        fileIO,
                        partitionManager,
                        Collections.singletonMap(
                                CoreOptions.FORMAT_TABLE_COMMIT_CLEANUP_THREAD_NUM.key(), "1"),
                        Collections.singletonMap("part", "p"));

        commit.commit(Collections.singletonList(new TwoPhaseCommitMessage(committer)));

        assertThat(fileIO.deleteCalls()).isEqualTo(3);
        assertThat(fileIO.maxConcurrentDeletes()).isEqualTo(1);
    }

    @Test
    void testCatalogManagedBuilderPropagatesConfiguredCleanupConcurrency() throws Exception {
        ParallelDeleteFileIO fileIO = new ParallelDeleteFileIO(7, true);
        Path tablePath = new Path(tempDir.toUri());
        Path partitionPath = new Path(tablePath, "part=p");
        writeOldFiles(fileIO, partitionPath, 8);
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        FormatTableCommit commit =
                builderOverwriteCommit(
                        tablePath,
                        fileIO,
                        partitionManager,
                        Collections.singletonMap(
                                CoreOptions.FORMAT_TABLE_COMMIT_CLEANUP_THREAD_NUM.key(), "7"),
                        Collections.singletonMap("part", "p"));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> result = executor.submit(() -> commit.commit(Collections.emptyList()));

            assertThat(fileIO.awaitFirstWave()).isTrue();
            assertThat(fileIO.awaitUnexpectedExtraDelete()).isFalse();

            fileIO.releaseFirstWave();
            result.get(10, TimeUnit.SECONDS);
            assertThat(fileIO.deleteCalls()).isEqualTo(8);
            assertThat(fileIO.maxConcurrentDeletes()).isEqualTo(7);
        } finally {
            fileIO.releaseFirstWave();
            executor.shutdownNow();
        }
    }

    @Test
    void testFilesystemDiscoveredFormatTableCleanupRemainsSerial() throws Exception {
        SerialProbeFileIO fileIO = new SerialProbeFileIO();
        Path tablePath = new Path(tempDir.toUri());
        Path partitionPath = new Path(tablePath, "part=p");
        writeOldFiles(fileIO, partitionPath, 3);
        TwoPhaseOutputStream.Committer committer = mock(TwoPhaseOutputStream.Committer.class);
        when(committer.targetPath()).thenReturn(new Path(partitionPath, "data-new.csv"));
        FormatTableCommit commit =
                builderOverwriteCommit(
                        tablePath,
                        fileIO,
                        null,
                        Collections.singletonMap(
                                CoreOptions.FORMAT_TABLE_COMMIT_CLEANUP_THREAD_NUM.key(), "64"),
                        Collections.singletonMap("part", "p"));

        commit.commit(Collections.singletonList(new TwoPhaseCommitMessage(committer)));

        assertThat(fileIO.deleteCalls()).isEqualTo(3);
        assertThat(fileIO.maxConcurrentDeletes()).isEqualTo(1);
    }

    @Test
    void testCleanupIsAHardBarrierBeforePublishingNewFiles() throws Exception {
        PartialBarrierDeleteFileIO fileIO = new PartialBarrierDeleteFileIO();
        Path tablePath = new Path(tempDir.toUri());
        Path partitionPath = new Path(tablePath, "part=p");
        writeOldFiles(fileIO, partitionPath, 2);
        TwoPhaseOutputStream.Committer committer = mock(TwoPhaseOutputStream.Committer.class);
        when(committer.targetPath()).thenReturn(new Path(partitionPath, "data-new.csv"));
        doAnswer(
                        invocation -> {
                            assertThat(fileIO.activeDeletes()).isZero();
                            return null;
                        })
                .when(committer)
                .commit(fileIO);
        FormatTableCommit commit =
                newCleanupCommit(tablePath, fileIO, null, Collections.singletonMap("part", "p"), 2);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> result =
                    executor.submit(
                            () ->
                                    commit.commit(
                                            Collections.singletonList(
                                                    new TwoPhaseCommitMessage(committer))));

            assertThat(fileIO.awaitBothDeletesStarted()).isTrue();
            verify(committer, never()).commit(fileIO);

            fileIO.releaseFirstDelete();
            assertThat(fileIO.awaitFirstDeleteReturned()).isTrue();
            assertThatThrownBy(() -> result.get(300, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            verify(committer, never()).commit(fileIO);

            fileIO.releaseSecondDelete();
            result.get(10, TimeUnit.SECONDS);
            verify(committer).commit(fileIO);
        } finally {
            fileIO.releaseFirstDelete();
            fileIO.releaseSecondDelete();
            executor.shutdownNow();
        }
    }

    @Test
    void testUnpartitionedCatalogManagedFormatTableCleanupRemainsSerial() throws Exception {
        SerialProbeFileIO fileIO = new SerialProbeFileIO();
        Path tablePath = new Path(tempDir.toUri());
        writeOldFiles(fileIO, tablePath, 3);
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        TwoPhaseOutputStream.Committer committer = mock(TwoPhaseOutputStream.Committer.class);
        when(committer.targetPath()).thenReturn(new Path(tablePath, "data-new.csv"));
        FormatTableCommit commit =
                builderUnpartitionedOverwriteCommit(
                        tablePath,
                        fileIO,
                        partitionManager,
                        Collections.singletonMap(
                                CoreOptions.FORMAT_TABLE_COMMIT_CLEANUP_THREAD_NUM.key(), "64"));

        commit.commit(Collections.singletonList(new TwoPhaseCommitMessage(committer)));

        assertThat(fileIO.deleteCalls()).isEqualTo(3);
        assertThat(fileIO.maxConcurrentDeletes()).isEqualTo(1);
    }

    @Test
    void testCleanupFailureStopsNewSubmissionsAndDrainsTheAlreadyRunningDelete() throws Exception {
        FailureDrainFileIO fileIO = new FailureDrainFileIO();
        Path tablePath = new Path(tempDir.toUri());
        Path partitionPath = new Path(tablePath, "part=p");
        writeOldFiles(fileIO, partitionPath, 6);
        TwoPhaseOutputStream.Committer committer = mock(TwoPhaseOutputStream.Committer.class);
        when(committer.targetPath()).thenReturn(new Path(partitionPath, "data-new.csv"));
        FormatTableCommit commit =
                newCleanupCommit(tablePath, fileIO, null, Collections.singletonMap("part", "p"), 2);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> result =
                    executor.submit(
                            () ->
                                    commit.commit(
                                            Collections.singletonList(
                                                    new TwoPhaseCommitMessage(committer))));

            assertThat(fileIO.awaitFailureAttempted()).isTrue();
            assertThatThrownBy(() -> result.get(300, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            assertThat(fileIO.attemptedFiles())
                    .containsExactlyInAnyOrder("data-000.csv", "data-001.csv");

            fileIO.releaseSuccessfulSibling();
            assertThat(getRootCause(awaitFailure(result)))
                    .hasMessage("delete failed at input position 0");
            assertThat(fileIO.attemptedFiles())
                    .containsExactlyInAnyOrder("data-000.csv", "data-001.csv", "data-new.csv");
            assertThat(fileIO.successfulFiles()).containsExactly("data-001.csv");
            verify(committer, never()).commit(fileIO);
        } finally {
            fileIO.releaseSuccessfulSibling();
            executor.shutdownNow();
        }
    }

    @Test
    void testLaterPartitionListingFailureDrainsAcceptedDeletesBeforeAbort() throws Exception {
        Path tablePath = new Path(new Path(tempDir.toUri()), "listing-failure");
        Path firstPartition = new Path(tablePath, "part=p0");
        Path failingPartition = new Path(tablePath, "part=p1");
        LaterRootListingFailureFileIO fileIO =
                new LaterRootListingFailureFileIO(failingPartition, 2);
        writeOldFiles(fileIO, firstPartition, 2);
        writeOldFiles(fileIO, failingPartition, 1);

        AtomicInteger discardCalls = new AtomicInteger();
        ConcurrentLinkedQueue<Integer> activeDeletesAtDiscard = new ConcurrentLinkedQueue<>();
        List<CommitMessage> messages = new ArrayList<>();
        List<TwoPhaseOutputStream.Committer> committers = new ArrayList<>();
        for (Path partition : Arrays.asList(firstPartition, failingPartition)) {
            TwoPhaseOutputStream.Committer committer = mock(TwoPhaseOutputStream.Committer.class);
            when(committer.targetPath()).thenReturn(new Path(partition, "data-new.csv"));
            doAnswer(
                            invocation -> {
                                activeDeletesAtDiscard.add(fileIO.activeDeletes());
                                discardCalls.incrementAndGet();
                                return null;
                            })
                    .when(committer)
                    .discard(fileIO);
            committers.add(committer);
            messages.add(new TwoPhaseCommitMessage(committer));
        }
        FormatTableCommit commit = newCleanupCommit(tablePath, fileIO, null, null, 3);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> result = executor.submit(() -> commit.commit(messages));
        try {
            assertThat(fileIO.awaitListingFailure()).isTrue();
            assertThatThrownBy(() -> result.get(300, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            assertThat(discardCalls).hasValue(0);
            for (TwoPhaseOutputStream.Committer committer : committers) {
                verify(committer, never()).commit(fileIO);
                verify(committer, never()).discard(fileIO);
            }

            fileIO.releaseFirstDelete();
            assertThat(fileIO.awaitFirstDeleteReturned()).isTrue();
            assertThatThrownBy(() -> result.get(300, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            assertThat(discardCalls).hasValue(0);

            fileIO.releaseSecondDelete();
            assertThat(getRootCause(awaitFailure(result)))
                    .hasMessage("Failed to list the later partition root.");
            assertThat(discardCalls).hasValue(2);
            assertThat(activeDeletesAtDiscard).containsExactly(0, 0);
            for (TwoPhaseOutputStream.Committer committer : committers) {
                verify(committer, never()).commit(fileIO);
                verify(committer).discard(fileIO);
            }
        } finally {
            fileIO.releaseFirstDelete();
            fileIO.releaseSecondDelete();
            try {
                if (!result.isDone()) {
                    try {
                        result.get(10, TimeUnit.SECONDS);
                    } catch (ExecutionException ignored) {
                        // The test expects the listing failure above.
                    }
                }
            } finally {
                executor.shutdownNow();
            }
        }
    }

    @Test
    void testCleanupSelectsLowestInputFailureAndSuppressesTheOtherFailure() throws Exception {
        OrderedDualFailureFileIO fileIO = new OrderedDualFailureFileIO();
        Path tablePath = new Path(tempDir.toUri());
        Path partitionPath = new Path(tablePath, "part=p");
        writeOldFiles(fileIO, partitionPath, 2);
        FormatTableCommit commit =
                newCleanupCommit(tablePath, fileIO, null, Collections.singletonMap("part", "p"), 2);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> result = executor.submit(() -> commit.commit(Collections.emptyList()));
            assertThat(fileIO.awaitHigherPositionFailure()).isTrue();
            assertThatThrownBy(() -> result.get(300, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);

            fileIO.releaseLowerPositionFailure();
            Throwable primary = getRootCause(awaitFailure(result));
            assertThat(primary).hasMessage("delete failed at input position 0");
            assertThat(primary.getSuppressed())
                    .extracting(Throwable::getMessage)
                    .containsExactly("delete failed at input position 1");
        } finally {
            fileIO.releaseLowerPositionFailure();
            executor.shutdownNow();
        }
    }

    @Test
    void testInterruptDrainsCleanupRestoresFlagAndNeverPublishes() throws Exception {
        BlockingDeleteFileIO fileIO = new BlockingDeleteFileIO(2);
        Path tablePath = new Path(tempDir.toUri());
        Path partitionPath = new Path(tablePath, "part=p");
        writeOldFiles(fileIO, partitionPath, 2);
        TwoPhaseOutputStream.Committer committer = mock(TwoPhaseOutputStream.Committer.class);
        when(committer.targetPath()).thenReturn(new Path(partitionPath, "data-new.csv"));
        FormatTableCommit commit =
                newCleanupCommit(tablePath, fileIO, null, Collections.singletonMap("part", "p"), 2);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean interruptRestored = new AtomicBoolean();
        CountDownLatch commitReturned = new CountDownLatch(1);
        Thread commitThread =
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
                                commitReturned.countDown();
                            }
                        },
                        "format-cleanup-interrupted-caller");

        commitThread.start();
        try {
            assertThat(fileIO.awaitDeletesStarted()).isTrue();
            commitThread.interrupt();
            assertThat(commitReturned.await(300, TimeUnit.MILLISECONDS)).isFalse();

            fileIO.releaseDeletes();
            assertThat(commitReturned.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(failure.get()).isNotNull();
            assertThat(getCausalChain(failure.get()))
                    .anyMatch(InterruptedException.class::isInstance);
            assertThat(interruptRestored).isTrue();
            verify(committer, never()).commit(fileIO);
        } finally {
            fileIO.releaseDeletes();
            commitThread.interrupt();
            commitThread.join(TimeUnit.SECONDS.toMillis(10));
        }
        assertThat(commitThread.isAlive()).isFalse();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testCleanupStatisticsClaimOnlyFilesDeletedByThisCommit() throws Exception {
        MixedOwnershipFileIO fileIO = new MixedOwnershipFileIO();
        Path tablePath = new Path(tempDir.toUri());
        writeOldFiles(fileIO, new Path(tablePath, "year=2025/month=00"), 1);
        writeOldFiles(fileIO, new Path(tablePath, "year=2025/month=01"), 1);
        writeOldFiles(fileIO, new Path(tablePath, "year=2025/month=02"), 1);
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        FormatTableCommit commit =
                new FormatTableCommit(
                        tablePath.toString(),
                        Arrays.asList("year", "month"),
                        fileIO,
                        false,
                        PARTITION_DEFAULT_NAME.defaultValue(),
                        true,
                        Identifier.create("cleanup_db", "cleanup_table"),
                        Collections.singletonMap("year", "2025"),
                        null,
                        null,
                        partitionManager,
                        /* dynamicPartitionOverwrite */ true,
                        2);

        commit.commit(Collections.emptyList());

        Map<String, String> owned = partitionSpec("2025", "00");
        ArgumentCaptor<List<Map<String, String>>> specs =
                ArgumentCaptor.forClass((Class) List.class);
        ArgumentCaptor<List<PartitionStatistics>> statistics =
                ArgumentCaptor.forClass((Class) List.class);
        verify(partitionManager)
                .createPartitions(specs.capture(), eq(true), statistics.capture(), eq(true));
        assertThat(specs.getValue()).containsExactly(owned);
        assertThat(statistics.getValue())
                .singleElement()
                .satisfies(
                        stat -> {
                            assertThat(stat.spec()).isEqualTo(owned);
                            assertThat(stat.recordCount()).isZero();
                            assertThat(stat.fileSizeInBytes()).isZero();
                            assertThat(stat.fileCount()).isZero();
                        });
        assertThat(fileIO.exists(new Path(tablePath, "year=2025/month=00/data-000.csv"))).isFalse();
        assertThat(fileIO.exists(new Path(tablePath, "year=2025/month=01/data-000.csv"))).isFalse();
        assertThat(fileIO.exists(new Path(tablePath, "year=2025/month=02/data-000.csv"))).isFalse();
    }

    @Test
    void testCleanupRejectsFalseWhenTheOldDataFileStillExists() throws Exception {
        RefusingDeleteFileIO fileIO = new RefusingDeleteFileIO();
        Path tablePath = new Path(tempDir.toUri());
        Path partitionPath = new Path(tablePath, "part=p");
        writeOldFiles(fileIO, partitionPath, 1);
        TwoPhaseOutputStream.Committer committer = mock(TwoPhaseOutputStream.Committer.class);
        when(committer.targetPath()).thenReturn(new Path(partitionPath, "data-new.csv"));
        FormatTableCommit commit =
                newCleanupCommit(tablePath, fileIO, null, Collections.singletonMap("part", "p"), 2);

        assertThatThrownBy(
                        () ->
                                commit.commit(
                                        Collections.singletonList(
                                                new TwoPhaseCommitMessage(committer))))
                .hasRootCauseMessage(
                        "Failed to delete data file "
                                + new Path(partitionPath, "data-000.csv")
                                + " of table cleanup_db.cleanup_table.");
        verify(committer, never()).commit(fileIO);
        verify(committer).discard(fileIO);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testConcurrentCleanupReportsCompleteStatisticsAfterBarrier() throws Exception {
        ParallelDeleteFileIO fileIO = new ParallelDeleteFileIO(4);
        Path tablePath = new Path(tempDir.toUri());
        for (int month = 0; month < 8; month++) {
            writeOldFiles(
                    fileIO, new Path(tablePath, String.format("year=2025/month=%02d", month)), 1);
        }
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        doAnswer(
                        invocation -> {
                            assertThat(fileIO.activeDeletes()).isZero();
                            return null;
                        })
                .when(partitionManager)
                .createPartitions(anyList(), eq(true), anyList(), eq(true));
        FormatTableCommit commit =
                new FormatTableCommit(
                        tablePath.toString(),
                        Arrays.asList("year", "month"),
                        fileIO,
                        false,
                        PARTITION_DEFAULT_NAME.defaultValue(),
                        true,
                        Identifier.create("cleanup_db", "cleanup_table"),
                        Collections.singletonMap("year", "2025"),
                        null,
                        null,
                        partitionManager,
                        /* dynamicPartitionOverwrite */ true,
                        4);

        commit.commit(Collections.emptyList());

        List<Map<String, String>> expectedSpecs = new ArrayList<>();
        for (int month = 0; month < 8; month++) {
            expectedSpecs.add(partitionSpec("2025", String.format("%02d", month)));
        }
        ArgumentCaptor<List<Map<String, String>>> specs =
                ArgumentCaptor.forClass((Class) List.class);
        ArgumentCaptor<List<PartitionStatistics>> statistics =
                ArgumentCaptor.forClass((Class) List.class);
        verify(partitionManager)
                .createPartitions(specs.capture(), eq(true), statistics.capture(), eq(true));
        assertThat(specs.getValue()).containsExactlyInAnyOrderElementsOf(expectedSpecs);
        assertThat(statistics.getValue())
                .hasSize(8)
                .extracting(PartitionStatistics::spec)
                .containsExactlyInAnyOrderElementsOf(expectedSpecs);
        assertThat(statistics.getValue())
                .allSatisfy(
                        stat -> {
                            assertThat(stat.recordCount()).isZero();
                            assertThat(stat.fileSizeInBytes()).isZero();
                            assertThat(stat.fileCount()).isZero();
                        });
    }

    @Test
    void testCatalogManagedOverwriteCleanupSpansPartitionDirectories() throws Exception {
        assertOverwriteCleanupSpansPartitions(/* dynamicPartitionOverwrite */ true);
        assertOverwriteCleanupSpansPartitions(/* dynamicPartitionOverwrite */ false);
    }

    @Test
    void testCleanupDoesNotListEveryPartitionBeforeTheFirstDeleteWindowCompletes()
            throws Exception {
        Path tablePath = new Path(new Path(tempDir.toUri()), "lazy-root-listing");
        Path firstPartition = new Path(tablePath, "part=p0");
        Path deferredPartition = new Path(tablePath, "part=p1");
        LazyRootListingFileIO fileIO = new LazyRootListingFileIO(2, deferredPartition);
        writeOldFiles(fileIO, firstPartition, 2);
        writeOldFiles(fileIO, deferredPartition, 2);

        List<CommitMessage> messages = new ArrayList<>();
        for (Path partition : Arrays.asList(firstPartition, deferredPartition)) {
            TwoPhaseOutputStream.Committer committer = mock(TwoPhaseOutputStream.Committer.class);
            when(committer.targetPath()).thenReturn(new Path(partition, "data-new.csv"));
            messages.add(new TwoPhaseCommitMessage(committer));
        }
        Map<String, String> options = new LinkedHashMap<>();
        options.put(CoreOptions.FORMAT_TABLE_COMMIT_CLEANUP_THREAD_NUM.key(), "2");
        FormatTableCommit commit =
                builderOverwriteCommit(
                        tablePath,
                        fileIO,
                        mock(FormatTablePartitionManager.class),
                        options,
                        /* staticPartition */ null);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> result = null;
        try {
            result = executor.submit(() -> commit.commit(messages));
            assertThat(fileIO.awaitFirstWave()).isTrue();
            assertThat(fileIO.deferredRootListed()).isFalse();
        } finally {
            fileIO.releaseFirstWave();
            if (result != null) {
                result.get(10, TimeUnit.SECONDS);
            }
            executor.shutdownNow();
        }

        assertThat(fileIO.deferredRootListed()).isTrue();
        assertThat(fileIO.deleteCalls()).isEqualTo(4);
    }

    @Test
    void testBuilderCleanupConcurrencyDoesNotApplyToTruncateOperations() throws Exception {
        SerialProbeFileIO tableFileIO = new SerialProbeFileIO();
        Path tablePath = new Path(new Path(tempDir.toUri()), "truncate-table");
        Path tablePartitionPath = new Path(tablePath, "part=p");
        writeOldFiles(tableFileIO, tablePartitionPath, 3);
        FormatTablePartitionManager tableManager = mock(FormatTablePartitionManager.class);
        when(tableManager.listPartitions(Collections.emptyMap(), null))
                .thenReturn(
                        Collections.singletonList(
                                new Partition(
                                        Collections.singletonMap("part", "p"),
                                        0,
                                        0,
                                        0,
                                        0,
                                        -1,
                                        false)));
        builderTruncateCommit(tablePath, tableFileIO, tableManager).truncateTable();

        SerialProbeFileIO partitionFileIO = new SerialProbeFileIO();
        Path partitionsPath = new Path(new Path(tempDir.toUri()), "truncate-partitions");
        Path namedPartitionPath = new Path(partitionsPath, "part=p");
        writeOldFiles(partitionFileIO, namedPartitionPath, 3);
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        when(partitionManager.listPartitionsByNames(anyList()))
                .thenReturn(
                        Collections.singletonList(
                                new Partition(
                                        Collections.singletonMap("part", "p"),
                                        0,
                                        0,
                                        0,
                                        0,
                                        -1,
                                        false)));
        builderTruncateCommit(partitionsPath, partitionFileIO, partitionManager)
                .truncatePartitions(
                        Collections.singletonList(Collections.singletonMap("part", "p")));

        assertThat(tableFileIO.maxConcurrentDeletes()).isEqualTo(1);
        assertThat(partitionFileIO.maxConcurrentDeletes()).isEqualTo(1);
    }

    @Test
    void testAbortFailureDoesNotReplaceInterruptedCleanupFailure() throws Exception {
        BlockingDeleteFileIO fileIO = new BlockingDeleteFileIO(2);
        Path tablePath = new Path(new Path(tempDir.toUri()), "abort-failure");
        Path partitionPath = new Path(tablePath, "part=p");
        writeOldFiles(fileIO, partitionPath, 2);
        TwoPhaseOutputStream.Committer committer = mock(TwoPhaseOutputStream.Committer.class);
        when(committer.targetPath()).thenReturn(new Path(partitionPath, "data-new.csv"));
        doThrow(new IOException("discard failed after cleanup interruption"))
                .when(committer)
                .discard(fileIO);
        FormatTableCommit commit =
                newCleanupCommit(tablePath, fileIO, null, Collections.singletonMap("part", "p"), 2);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean interruptRestored = new AtomicBoolean();
        Thread commitThread =
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
                            }
                        },
                        "format-cleanup-abort-failure-caller");

        commitThread.start();
        try {
            assertThat(fileIO.awaitDeletesStarted()).isTrue();
            commitThread.interrupt();
            fileIO.releaseDeletes();
            commitThread.join(TimeUnit.SECONDS.toMillis(10));
        } finally {
            fileIO.releaseDeletes();
            commitThread.interrupt();
            commitThread.join(TimeUnit.SECONDS.toMillis(10));
        }

        assertThat(commitThread.isAlive()).isFalse();
        assertThat(failure.get()).isNotNull();
        assertThat(getRootCause(failure.get())).isInstanceOf(InterruptedException.class);
        assertThat(failureTree(failure.get()))
                .extracting(Throwable::getMessage)
                .contains("discard failed after cleanup interruption");
        assertThat(interruptRestored).isTrue();
        verify(committer).discard(fileIO);
        verify(committer, never()).commit(fileIO);
    }

    /**
     * An overwrite that names no partition: what INSERT OVERWRITE without a PARTITION clause is.
     */
    private FormatTableCommit overwritingCommit(
            Path tableLocation,
            LocalFileIO fileIO,
            boolean dynamicPartitionOverwrite,
            String... partitionKeys) {
        return new FormatTableCommit(
                tableLocation.toString(),
                Arrays.asList(partitionKeys),
                fileIO,
                false,
                PARTITION_DEFAULT_NAME.defaultValue(),
                true,
                Identifier.create("overwrite_db", "overwrite_table"),
                null,
                null,
                null,
                null,
                dynamicPartitionOverwrite,
                /* cleanupThreadNum */ 1);
    }

    private FormatTableCommit staticPartitionOverwriteCommit(
            Path tableLocation, FileIO fileIO, int publishThreadNum) {
        return new FormatTableCommit(
                tableLocation.toString(),
                Collections.singletonList("part"),
                fileIO,
                false,
                PARTITION_DEFAULT_NAME.defaultValue(),
                true,
                Identifier.create("overwrite_db", "overwrite_table"),
                Collections.singletonMap("part", "p"),
                null,
                null,
                null,
                /* dynamicPartitionOverwrite */ true,
                /* cleanupThreadNum */ 1,
                publishThreadNum);
    }

    private void assertOverwriteCleanupSpansPartitions(boolean dynamicPartitionOverwrite)
            throws Exception {
        ParallelDeleteFileIO fileIO = new ParallelDeleteFileIO(4);
        Path tablePath =
                new Path(
                        new Path(tempDir.toUri()),
                        dynamicPartitionOverwrite ? "dynamic-roots" : "whole-roots");
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        List<Partition> partitions = new ArrayList<>();
        List<CommitMessage> messages = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            String value = "p" + i;
            Path partitionPath = new Path(tablePath, "part=" + value);
            writeOldFiles(fileIO, partitionPath, 1);
            partitions.add(
                    new Partition(Collections.singletonMap("part", value), 0, 0, 0, 0, -1, false));
            if (dynamicPartitionOverwrite) {
                TwoPhaseOutputStream.Committer committer =
                        mock(TwoPhaseOutputStream.Committer.class);
                when(committer.targetPath()).thenReturn(new Path(partitionPath, "data-new.csv"));
                messages.add(new TwoPhaseCommitMessage(committer));
            }
        }
        if (!dynamicPartitionOverwrite) {
            when(partitionManager.listPartitions(Collections.emptyMap(), null))
                    .thenReturn(partitions);
        }
        Map<String, String> options = new LinkedHashMap<>();
        options.put(CoreOptions.FORMAT_TABLE_COMMIT_CLEANUP_THREAD_NUM.key(), "4");
        options.put(
                CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key(),
                Boolean.toString(dynamicPartitionOverwrite));
        FormatTableCommit commit =
                builderOverwriteCommit(
                        tablePath, fileIO, partitionManager, options, /* staticPartition */ null);

        commit.commit(messages);

        assertThat(fileIO.deleteCalls()).isEqualTo(4);
        assertThat(fileIO.maxConcurrentDeletes()).isEqualTo(4);
    }

    private FormatTableCommit builderOverwriteCommit(
            Path tablePath,
            FileIO fileIO,
            FormatTablePartitionManager partitionManager,
            Map<String, String> options,
            Map<String, String> staticPartition) {
        FormatTable table = formatTable(tablePath, fileIO, partitionManager, options);
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        writeBuilder.withOverwrite(staticPartition);
        return (FormatTableCommit) writeBuilder.newCommit();
    }

    private FormatTableCommit builderTruncateCommit(
            Path tablePath, FileIO fileIO, FormatTablePartitionManager partitionManager) {
        return (FormatTableCommit)
                formatTable(tablePath, fileIO, partitionManager, Collections.emptyMap())
                        .newBatchWriteBuilder()
                        .newCommit();
    }

    private FormatTable formatTable(
            Path tablePath,
            FileIO fileIO,
            FormatTablePartitionManager partitionManager,
            Map<String, String> options) {
        RowType rowType =
                RowType.builder()
                        .field("part", DataTypes.STRING())
                        .field("id", DataTypes.INT())
                        .build();
        FormatTable table =
                FormatTable.builder()
                        .fileIO(fileIO)
                        .identifier(Identifier.create("cleanup_db", "cleanup_table"))
                        .rowType(rowType)
                        .partitionKeys(Collections.singletonList("part"))
                        .location(tablePath.toString())
                        .format(FormatTable.Format.CSV)
                        .options(options)
                        .partitionManager(partitionManager)
                        .build();
        return table;
    }

    private FormatTableCommit builderUnpartitionedOverwriteCommit(
            Path tablePath,
            FileIO fileIO,
            FormatTablePartitionManager partitionManager,
            Map<String, String> options) {
        FormatTable table =
                FormatTable.builder()
                        .fileIO(fileIO)
                        .identifier(Identifier.create("cleanup_db", "cleanup_table"))
                        .rowType(RowType.builder().field("id", DataTypes.INT()).build())
                        .partitionKeys(Collections.emptyList())
                        .location(tablePath.toString())
                        .format(FormatTable.Format.CSV)
                        .options(options)
                        .partitionManager(partitionManager)
                        .build();
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        writeBuilder.withOverwrite(null);
        return (FormatTableCommit) writeBuilder.newCommit();
    }

    private FormatTableCommit newCleanupCommit(
            Path tablePath,
            FileIO fileIO,
            FormatTablePartitionManager partitionManager,
            Map<String, String> staticPartition,
            int cleanupThreadNum) {
        return new FormatTableCommit(
                tablePath.toString(),
                Collections.singletonList("part"),
                fileIO,
                false,
                PARTITION_DEFAULT_NAME.defaultValue(),
                true,
                Identifier.create("cleanup_db", "cleanup_table"),
                staticPartition,
                null,
                null,
                partitionManager,
                /* dynamicPartitionOverwrite */ true,
                cleanupThreadNum);
    }

    private static void writeOldFiles(LocalFileIO fileIO, Path partitionPath, int count)
            throws IOException {
        for (int i = 0; i < count; i++) {
            fileIO.writeFile(
                    new Path(partitionPath, String.format("data-%03d.csv", i)), "old", false);
        }
    }

    private static void assertPublishesOnCaller(
            FormatTableCommit commit, FileIO fileIO, Path parent) throws IOException {
        Thread caller = Thread.currentThread();
        ConcurrentLinkedQueue<Thread> publishThreads = new ConcurrentLinkedQueue<>();
        List<CommitMessage> messages = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            TwoPhaseOutputStream.Committer committer = mock(TwoPhaseOutputStream.Committer.class);
            when(committer.targetPath()).thenReturn(new Path(parent, "data-" + i + ".csv"));
            doAnswer(
                            invocation -> {
                                publishThreads.add(Thread.currentThread());
                                return null;
                            })
                    .when(committer)
                    .commit(fileIO);
            messages.add(new TwoPhaseCommitMessage(committer));
        }

        commit.commit(messages);

        assertThat(publishThreads).containsExactly(caller, caller);
    }

    private static ExecutionException awaitFailure(Future<?> future) throws Exception {
        try {
            future.get(10, TimeUnit.SECONDS);
            throw new AssertionError("Expected Format Table commit to fail");
        } catch (ExecutionException expected) {
            return expected;
        }
    }

    private static void awaitLatch(CountDownLatch latch, String description) throws IOException {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IOException("Timed out waiting for " + description);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for " + description, e);
        }
    }

    private static List<Throwable> failureTree(Throwable throwable) {
        List<Throwable> failures = new ArrayList<>();
        collectFailures(throwable, failures);
        return failures;
    }

    private static void collectFailures(Throwable throwable, List<Throwable> failures) {
        if (throwable == null) {
            return;
        }
        failures.add(throwable);
        for (Throwable suppressed : throwable.getSuppressed()) {
            collectFailures(suppressed, failures);
        }
        collectFailures(throwable.getCause(), failures);
    }

    private abstract static class StagedFileCommitter implements TwoPhaseOutputStream.Committer {

        private static final long serialVersionUID = 1L;

        private final Path targetPath;
        private final Path stagingPath;

        private StagedFileCommitter(Path targetPath, Path stagingPath) {
            this.targetPath = targetPath;
            this.stagingPath = stagingPath;
        }

        protected void publish(FileIO fileIO) throws IOException {
            fileIO.writeFile(targetPath, "replacement", false);
        }

        @Override
        public void discard(FileIO fileIO) {
            fileIO.deleteQuietly(targetPath);
            fileIO.deleteQuietly(stagingPath);
        }

        @Override
        public Path targetPath() {
            return targetPath;
        }

        @Override
        public void clean(FileIO fileIO) throws IOException {
            if (!fileIO.delete(stagingPath, false) && fileIO.exists(stagingPath)) {
                throw new IOException("Failed to clean staging file " + stagingPath);
            }
        }
    }

    private static class PostRegistrationFailingHiveCatalog extends FileSystemCatalog {

        private final List<Map<String, String>> registeredPartitions = new ArrayList<>();

        private PostRegistrationFailingHiveCatalog(FileIO fileIO, Path warehouse) {
            super(fileIO, warehouse);
        }

        public void createPartitionsUtil(
                Identifier identifier,
                List<Map<String, String>> partitions,
                boolean partitionOnlyValueInPath) {
            registeredPartitions.addAll(partitions);
            throw new RuntimeException("Hive failed after partition registration");
        }
    }

    private static class RecordingHiveCatalog extends FileSystemCatalog {

        private final List<Map<String, String>> registeredPartitions = new ArrayList<>();

        private RecordingHiveCatalog(FileIO fileIO, Path warehouse) {
            super(fileIO, warehouse);
        }

        public void createPartitionsUtil(
                Identifier identifier,
                List<Map<String, String>> partitions,
                boolean partitionOnlyValueInPath) {
            registeredPartitions.addAll(partitions);
        }
    }

    private static class SelectiveRefusingDeleteFileIO extends LocalFileIO {

        private static final long serialVersionUID = 1L;

        private final Path refusedPath;

        private SelectiveRefusingDeleteFileIO(Path refusedPath) {
            this.refusedPath = refusedPath;
        }

        @Override
        public boolean delete(Path path, boolean recursive) throws IOException {
            return path.equals(refusedPath) ? false : super.delete(path, recursive);
        }
    }

    private static class ParallelDeleteFileIO extends LocalFileIO {

        private final int firstWaveSize;
        private final boolean holdFirstWave;
        private final CountDownLatch firstWave;
        private final CountDownLatch releaseFirstWave = new CountDownLatch(1);
        private final CountDownLatch unexpectedExtraDelete = new CountDownLatch(1);
        private final AtomicInteger deleteCalls = new AtomicInteger();
        private final AtomicInteger activeDeletes = new AtomicInteger();
        private final AtomicInteger maxConcurrentDeletes = new AtomicInteger();

        private ParallelDeleteFileIO(int firstWaveSize) {
            this(firstWaveSize, false);
        }

        private ParallelDeleteFileIO(int firstWaveSize, boolean holdFirstWave) {
            this.firstWaveSize = firstWaveSize;
            this.holdFirstWave = holdFirstWave;
            this.firstWave = new CountDownLatch(firstWaveSize);
        }

        @Override
        public boolean delete(Path path, boolean recursive) throws IOException {
            int call = deleteCalls.incrementAndGet();
            int active = activeDeletes.incrementAndGet();
            maxConcurrentDeletes.updateAndGet(previous -> Math.max(previous, active));
            try {
                if (call <= firstWaveSize) {
                    firstWave.countDown();
                    if (!firstWave.await(10, TimeUnit.SECONDS)) {
                        throw new IOException("Expected cleanup delete calls did not overlap");
                    }
                    if (holdFirstWave && !releaseFirstWave.await(10, TimeUnit.SECONDS)) {
                        throw new IOException("Test did not release the first cleanup wave");
                    }
                } else {
                    unexpectedExtraDelete.countDown();
                }
                return super.delete(path, recursive);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while observing cleanup concurrency", e);
            } finally {
                activeDeletes.decrementAndGet();
            }
        }

        protected int deleteCalls() {
            return deleteCalls.get();
        }

        private int maxConcurrentDeletes() {
            return maxConcurrentDeletes.get();
        }

        private int activeDeletes() {
            return activeDeletes.get();
        }

        protected boolean awaitFirstWave() throws InterruptedException {
            return firstWave.await(10, TimeUnit.SECONDS);
        }

        private boolean awaitUnexpectedExtraDelete() throws InterruptedException {
            return unexpectedExtraDelete.await(300, TimeUnit.MILLISECONDS);
        }

        protected void releaseFirstWave() {
            releaseFirstWave.countDown();
        }
    }

    private static class LazyRootListingFileIO extends ParallelDeleteFileIO {

        private final Path deferredRoot;
        private final AtomicBoolean deferredRootListed = new AtomicBoolean();

        private LazyRootListingFileIO(int firstWaveSize, Path deferredRoot) {
            super(firstWaveSize, true);
            this.deferredRoot = deferredRoot;
        }

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            if (deferredRoot.equals(path)) {
                deferredRootListed.set(true);
            }
            return super.listStatus(path);
        }

        private boolean deferredRootListed() {
            return deferredRootListed.get();
        }
    }

    private static class SerialProbeFileIO extends LocalFileIO {

        private final CountDownLatch secondDeleteStarted = new CountDownLatch(1);
        private final AtomicInteger deleteCalls = new AtomicInteger();
        private final AtomicInteger activeDeletes = new AtomicInteger();
        private final AtomicInteger maxConcurrentDeletes = new AtomicInteger();

        @Override
        public boolean delete(Path path, boolean recursive) throws IOException {
            int call = deleteCalls.incrementAndGet();
            int active = activeDeletes.incrementAndGet();
            maxConcurrentDeletes.updateAndGet(previous -> Math.max(previous, active));
            try {
                if (call == 1) {
                    secondDeleteStarted.await(300, TimeUnit.MILLISECONDS);
                } else {
                    secondDeleteStarted.countDown();
                }
                return super.delete(path, recursive);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while probing serial cleanup", e);
            } finally {
                activeDeletes.decrementAndGet();
            }
        }

        private int deleteCalls() {
            return deleteCalls.get();
        }

        private int maxConcurrentDeletes() {
            return maxConcurrentDeletes.get();
        }
    }

    private static class PartialBarrierDeleteFileIO extends SortedLocalFileIO {

        private final CountDownLatch bothDeletesStarted = new CountDownLatch(2);
        private final CountDownLatch releaseFirstDelete = new CountDownLatch(1);
        private final CountDownLatch releaseSecondDelete = new CountDownLatch(1);
        private final CountDownLatch firstDeleteReturned = new CountDownLatch(1);
        private final AtomicInteger activeDeletes = new AtomicInteger();

        @Override
        public boolean delete(Path path, boolean recursive) throws IOException {
            activeDeletes.incrementAndGet();
            bothDeletesStarted.countDown();
            await(bothDeletesStarted, "both barrier delete calls");
            try {
                if ("data-000.csv".equals(path.getName())) {
                    await(releaseFirstDelete, "first barrier delete release");
                    return super.delete(path, recursive);
                }
                await(releaseSecondDelete, "second barrier delete release");
                return super.delete(path, recursive);
            } finally {
                activeDeletes.decrementAndGet();
                if ("data-000.csv".equals(path.getName())) {
                    firstDeleteReturned.countDown();
                }
            }
        }

        private boolean awaitBothDeletesStarted() throws InterruptedException {
            return bothDeletesStarted.await(10, TimeUnit.SECONDS);
        }

        private void releaseFirstDelete() {
            releaseFirstDelete.countDown();
        }

        private void releaseSecondDelete() {
            releaseSecondDelete.countDown();
        }

        private boolean awaitFirstDeleteReturned() throws InterruptedException {
            return firstDeleteReturned.await(10, TimeUnit.SECONDS);
        }

        private int activeDeletes() {
            return activeDeletes.get();
        }
    }

    private static class BlockingDeleteFileIO extends LocalFileIO {

        private final CountDownLatch deletesStarted;
        private final CountDownLatch releaseDeletes = new CountDownLatch(1);

        private BlockingDeleteFileIO(int deleteCount) {
            this.deletesStarted = new CountDownLatch(deleteCount);
        }

        @Override
        public boolean delete(Path path, boolean recursive) throws IOException {
            deletesStarted.countDown();
            try {
                if (!releaseDeletes.await(10, TimeUnit.SECONDS)) {
                    throw new IOException("Test did not release blocked cleanup deletes");
                }
                return super.delete(path, recursive);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while blocking cleanup delete", e);
            }
        }

        private boolean awaitDeletesStarted() throws InterruptedException {
            return deletesStarted.await(10, TimeUnit.SECONDS);
        }

        private void releaseDeletes() {
            releaseDeletes.countDown();
        }
    }

    private abstract static class SortedLocalFileIO extends LocalFileIO {

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            FileStatus[] statuses = super.listStatus(path);
            Arrays.sort(statuses, Comparator.comparing(status -> status.getPath().toString()));
            return statuses;
        }

        protected static void await(CountDownLatch latch, String description) throws IOException {
            try {
                if (!latch.await(10, TimeUnit.SECONDS)) {
                    throw new IOException("Timed out waiting for " + description);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for " + description, e);
            }
        }
    }

    private static class LaterRootListingFailureFileIO extends SortedLocalFileIO {

        private final Path failingRoot;
        private final CountDownLatch deletesStarted;
        private final CountDownLatch listingFailure = new CountDownLatch(1);
        private final CountDownLatch releaseFirstDelete = new CountDownLatch(1);
        private final CountDownLatch releaseSecondDelete = new CountDownLatch(1);
        private final CountDownLatch firstDeleteReturned = new CountDownLatch(1);
        private final AtomicInteger activeDeletes = new AtomicInteger();

        private LaterRootListingFailureFileIO(Path failingRoot, int deleteCount) {
            this.failingRoot = failingRoot;
            this.deletesStarted = new CountDownLatch(deleteCount);
        }

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            if (failingRoot.equals(path)) {
                await(deletesStarted, "accepted deletes before later-root listing failure");
                listingFailure.countDown();
                throw new IOException("Failed to list the later partition root.");
            }
            return super.listStatus(path);
        }

        @Override
        public boolean delete(Path path, boolean recursive) throws IOException {
            activeDeletes.incrementAndGet();
            deletesStarted.countDown();
            try {
                if ("data-000.csv".equals(path.getName())) {
                    await(releaseFirstDelete, "release of the first accepted delete");
                } else {
                    await(releaseSecondDelete, "release of the second accepted delete");
                }
                return super.delete(path, recursive);
            } finally {
                activeDeletes.decrementAndGet();
                if ("data-000.csv".equals(path.getName())) {
                    firstDeleteReturned.countDown();
                }
            }
        }

        private boolean awaitListingFailure() throws InterruptedException {
            return listingFailure.await(10, TimeUnit.SECONDS);
        }

        private void releaseFirstDelete() {
            releaseFirstDelete.countDown();
        }

        private boolean awaitFirstDeleteReturned() throws InterruptedException {
            return firstDeleteReturned.await(10, TimeUnit.SECONDS);
        }

        private void releaseSecondDelete() {
            releaseSecondDelete.countDown();
        }

        private int activeDeletes() {
            return activeDeletes.get();
        }
    }

    private static class FailureDrainFileIO extends SortedLocalFileIO {

        private final CountDownLatch firstPairStarted = new CountDownLatch(2);
        private final CountDownLatch failureAttempted = new CountDownLatch(1);
        private final CountDownLatch releaseSuccessfulSibling = new CountDownLatch(1);
        private final ConcurrentLinkedQueue<String> attemptedFiles = new ConcurrentLinkedQueue<>();
        private final ConcurrentLinkedQueue<String> successfulFiles = new ConcurrentLinkedQueue<>();

        @Override
        public boolean delete(Path path, boolean recursive) throws IOException {
            String name = path.getName();
            attemptedFiles.add(name);
            if ("data-000.csv".equals(name)) {
                firstPairStarted.countDown();
                await(firstPairStarted, "both initial deletes to start");
                failureAttempted.countDown();
                throw new IOException("delete failed at input position 0");
            }
            if ("data-001.csv".equals(name)) {
                firstPairStarted.countDown();
                await(firstPairStarted, "both initial deletes to start");
                await(releaseSuccessfulSibling, "release of in-flight sibling");
                boolean deleted = super.delete(path, recursive);
                successfulFiles.add(name);
                return deleted;
            }
            return super.delete(path, recursive);
        }

        private boolean awaitFailureAttempted() throws InterruptedException {
            return failureAttempted.await(10, TimeUnit.SECONDS);
        }

        private void releaseSuccessfulSibling() {
            releaseSuccessfulSibling.countDown();
        }

        private ConcurrentLinkedQueue<String> attemptedFiles() {
            return attemptedFiles;
        }

        private ConcurrentLinkedQueue<String> successfulFiles() {
            return successfulFiles;
        }
    }

    private static class OrderedDualFailureFileIO extends SortedLocalFileIO {

        private final CountDownLatch firstPairStarted = new CountDownLatch(2);
        private final CountDownLatch higherPositionFailure = new CountDownLatch(1);
        private final CountDownLatch releaseLowerPositionFailure = new CountDownLatch(1);

        @Override
        public boolean delete(Path path, boolean recursive) throws IOException {
            firstPairStarted.countDown();
            await(firstPairStarted, "both failing deletes to start");
            if ("data-001.csv".equals(path.getName())) {
                higherPositionFailure.countDown();
                throw new IOException("delete failed at input position 1");
            }
            await(releaseLowerPositionFailure, "lower-position failure");
            throw new IOException("delete failed at input position 0");
        }

        private boolean awaitHigherPositionFailure() throws InterruptedException {
            return higherPositionFailure.await(10, TimeUnit.SECONDS);
        }

        private void releaseLowerPositionFailure() {
            releaseLowerPositionFailure.countDown();
        }
    }

    private static class MixedOwnershipFileIO extends SortedLocalFileIO {

        @Override
        public boolean delete(Path path, boolean recursive) throws IOException {
            boolean deleted = super.delete(path, recursive);
            if (path.toString().contains("month=01")) {
                throw new FileNotFoundException("concurrently deleted " + path);
            }
            if (path.toString().contains("month=02")) {
                return false;
            }
            return deleted;
        }
    }

    private static class RefusingDeleteFileIO extends LocalFileIO {

        @Override
        public boolean delete(Path path, boolean recursive) {
            return false;
        }
    }

    private static Map<String, String> partitionSpec(String year, String month) {
        LinkedHashMap<String, String> spec = new LinkedHashMap<>();
        spec.put("year", year);
        spec.put("month", month);
        return spec;
    }

    /** The commit TRUNCATE makes: nothing to write, so no overwrite and no static partition. */
    private FormatTableCommit truncatingCommit(
            Path tableLocation,
            LocalFileIO fileIO,
            boolean onlyValueInPath,
            FormatTablePartitionManager partitionManager,
            String... partitionKeys) {
        return new FormatTableCommit(
                tableLocation.toString(),
                Arrays.asList(partitionKeys),
                fileIO,
                onlyValueInPath,
                PARTITION_DEFAULT_NAME.defaultValue(),
                false,
                Identifier.create("truncate_db", "truncate_table"),
                null,
                null,
                null,
                partitionManager,
                /* dynamicPartitionOverwrite */ true,
                /* cleanupThreadNum */ 1);
    }

    private FormatTablePartitionManager commitPartitionedFile(
            Path tableLocation, boolean onlyValueInPath, String partitionDir) throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path targetPath = new Path(new Path(tableLocation, partitionDir), "data-1.csv");
        RenamingTwoPhaseOutputStream outputStream =
                new RenamingTwoPhaseOutputStream(fileIO, targetPath, false);
        outputStream.write(1);
        TwoPhaseOutputStream.Committer committer = outputStream.closeForCommit();
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        FormatTableCommit commit =
                new FormatTableCommit(
                        tableLocation.toString(),
                        Arrays.asList("year", "month"),
                        fileIO,
                        onlyValueInPath,
                        PARTITION_DEFAULT_NAME.defaultValue(),
                        false,
                        Identifier.create("catalog_partition_db", "catalog_partition_table"),
                        null,
                        null,
                        null,
                        partitionManager,
                        /* dynamicPartitionOverwrite */ true,
                        /* cleanupThreadNum */ 1);
        commit.commit(Collections.singletonList(new TwoPhaseCommitMessage(committer)));
        return partitionManager;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Map<String, String> registeredSpec(
            FormatTablePartitionManager partitionManager) {
        ArgumentCaptor<List<Map<String, String>>> captor =
                ArgumentCaptor.forClass((Class) List.class);
        verify(partitionManager).createPartitions(captor.capture(), eq(true), any(), anyBoolean());
        assertThat(captor.getValue()).hasSize(1);
        return captor.getValue().get(0);
    }
}
