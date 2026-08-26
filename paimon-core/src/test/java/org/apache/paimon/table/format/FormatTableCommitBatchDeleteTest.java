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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.partition.Partition;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.CoreOptions.PARTITION_DEFAULT_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests that a Format Table overwrite hands whole batches of old files to a file system that
 * deletes in batches, and that every path which cannot report what it deleted keeps deleting one
 * file at a time.
 */
class FormatTableCommitBatchDeleteTest {

    private static final String PARTITION = "part=2025";

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testDynamicOverwriteHandsOldFilesOverInOneBatch() throws Exception {
        BatchDeletingFileIO fileIO = new BatchDeletingFileIO(true);
        Path tablePath = new Path(tempDir.toUri());
        Path partitionPath = new Path(tablePath, PARTITION);
        writeOldFiles(fileIO, partitionPath, 5);

        commit(
                newCommit(
                        tablePath, fileIO, mock(FormatTablePartitionManager.class), null, true, 8),
                fileIO,
                partitionPath);

        assertThat(fileIO.batchSizes).containsExactly(1, 4);
        assertThat(fileIO.singleDeletes).isEmpty();
        assertThat(oldFilesLeft(fileIO, partitionPath)).isZero();
    }

    @Test
    void testBatchesStayBoundedSoListingCanStayLazy() throws Exception {
        BatchDeletingFileIO fileIO = new BatchDeletingFileIO(true);
        Path tablePath = new Path(tempDir.toUri());
        Path partitionPath = new Path(tablePath, PARTITION);
        writeOldFiles(fileIO, partitionPath, 1001);

        commit(
                newCommit(
                        tablePath, fileIO, mock(FormatTablePartitionManager.class), null, true, 8),
                fileIO,
                partitionPath);

        // A batch is handed over as soon as it is full, so an overwrite that replaces many
        // partitions never holds every file the table has.
        assertThat(fileIO.batchSizes).containsExactly(1, 1000);
        assertThat(oldFilesLeft(fileIO, partitionPath)).isZero();
    }

    @Test
    void testFileSystemWithoutBatchDeleteStillDeletesEveryOldFile() throws Exception {
        BatchDeletingFileIO fileIO = new BatchDeletingFileIO(false);
        Path tablePath = new Path(tempDir.toUri());
        Path partitionPath = new Path(tablePath, PARTITION);
        writeOldFiles(fileIO, partitionPath, 5);

        commit(
                newCommit(
                        tablePath, fileIO, mock(FormatTablePartitionManager.class), null, true, 8),
                fileIO,
                partitionPath);

        // Refusing the batch means no storage was touched, so the files are all still there to
        // delete one at a time.
        assertThat(fileIO.batchSizes).containsExactly(1);
        assertThat(fileIO.singleDeletes).hasSize(5);
        assertThat(oldFilesLeft(fileIO, partitionPath)).isZero();
    }

    @Test
    void testARefusedLaterBatchLeavesNothingBehind() throws Exception {
        // A file system that stops deleting in batches half way is not what the contract allows,
        // but falling back has to converge anyway: the files the first batch removed are simply
        // not listed again.
        BatchDeletingFileIO fileIO = new BatchDeletingFileIO(true);
        fileIO.refuseBatchFrom = 3;
        Path tablePath = new Path(tempDir.toUri());
        Path partitionPath = new Path(tablePath, PARTITION);
        writeOldFiles(fileIO, partitionPath, 1500);

        commit(
                newCommit(
                        tablePath, fileIO, mock(FormatTablePartitionManager.class), null, true, 8),
                fileIO,
                partitionPath);

        assertThat(fileIO.batchSizes).containsExactly(1, 1000, 499);
        assertThat(fileIO.singleDeletes).hasSize(499);
        assertThat(oldFilesLeft(fileIO, partitionPath)).isZero();
    }

    @Test
    void testBatchDeleteFailureFailsTheCommitWithoutDeletingFileByFile() throws Exception {
        BatchDeletingFileIO fileIO = new BatchDeletingFileIO(true);
        fileIO.failure = new IOException("batch delete rejected");
        Path tablePath = new Path(tempDir.toUri());
        Path partitionPath = new Path(tablePath, PARTITION);
        writeOldFiles(fileIO, partitionPath, 5);
        FormatTableCommit commit =
                newCommit(
                        tablePath, fileIO, mock(FormatTablePartitionManager.class), null, true, 8);
        TwoPhaseOutputStream.Committer committer = committer(partitionPath);

        assertThatThrownBy(
                        () ->
                                commit.commit(
                                        Collections.singletonList(
                                                new TwoPhaseCommitMessage(committer))))
                .hasRootCauseMessage("batch delete rejected");

        // Once the file system has the batch, a failure is the answer. Deleting the same files
        // again one by one would hide how much of the batch went through.
        assertThat(fileIO.singleDeletes).hasSize(0);
        assertThat(oldFilesLeft(fileIO, partitionPath)).isEqualTo(5);
        verify(committer).discard(fileIO);
    }

    @Test
    void testStaticPartitionOverwriteDeletesFileByFile() throws Exception {
        BatchDeletingFileIO fileIO = new BatchDeletingFileIO(true);
        Path tablePath = new Path(tempDir.toUri());
        Path partitionPath = new Path(tablePath, PARTITION);
        writeOldFiles(fileIO, partitionPath, 5);

        commit(
                newCommit(
                        tablePath,
                        fileIO,
                        mock(FormatTablePartitionManager.class),
                        Collections.singletonMap("part", "2025"),
                        true,
                        8),
                fileIO,
                partitionPath);

        // A static overwrite reports the partitions it cleared, and a batch cannot say which
        // files it found.
        assertThat(fileIO.batchSizes).isEmpty();
        assertThat(fileIO.singleDeletes).hasSize(5);
    }

    @Test
    void testWholeTableOverwriteDeletesFileByFile() throws Exception {
        BatchDeletingFileIO fileIO = new BatchDeletingFileIO(true);
        Path tablePath = new Path(tempDir.toUri());
        Path partitionPath = new Path(tablePath, PARTITION);
        writeOldFiles(fileIO, partitionPath, 5);

        commit(
                newCommit(tablePath, fileIO, registeredPartition(), null, false, 8),
                fileIO,
                partitionPath);

        assertThat(fileIO.batchSizes).isEmpty();
        assertThat(fileIO.singleDeletes).hasSize(5);
    }

    @Test
    void testTableWithoutCatalogManagedPartitionsDeletesFileByFile() throws Exception {
        BatchDeletingFileIO fileIO = new BatchDeletingFileIO(true);
        Path tablePath = new Path(tempDir.toUri());
        Path partitionPath = new Path(tablePath, PARTITION);
        writeOldFiles(fileIO, partitionPath, 5);

        commit(newCommit(tablePath, fileIO, null, null, true, 8), fileIO, partitionPath);

        assertThat(fileIO.batchSizes).isEmpty();
        assertThat(fileIO.singleDeletes).hasSize(5);
    }

    @Test
    void testSerialCleanupDeletesFileByFile() throws Exception {
        BatchDeletingFileIO fileIO = new BatchDeletingFileIO(true);
        Path tablePath = new Path(tempDir.toUri());
        Path partitionPath = new Path(tablePath, PARTITION);
        writeOldFiles(fileIO, partitionPath, 5);

        // One cleanup thread is the opt out of everything this option family changed.
        commit(
                newCommit(
                        tablePath, fileIO, mock(FormatTablePartitionManager.class), null, true, 1),
                fileIO,
                partitionPath);

        assertThat(fileIO.batchSizes).isEmpty();
        assertThat(fileIO.singleDeletes).hasSize(5);
    }

    @Test
    void testUnpartitionedTableDeletesFileByFile() throws Exception {
        BatchDeletingFileIO fileIO = new BatchDeletingFileIO(true);
        Path tablePath = new Path(tempDir.toUri());
        writeOldFiles(fileIO, tablePath, 5);
        FormatTableCommit commit =
                new FormatTableCommit(
                        tablePath.toString(),
                        Collections.emptyList(),
                        fileIO,
                        false,
                        PARTITION_DEFAULT_NAME.defaultValue(),
                        true,
                        Identifier.create("batch_db", "batch_table"),
                        null,
                        null,
                        null,
                        mock(FormatTablePartitionManager.class),
                        true,
                        8);

        commit(commit, fileIO, tablePath);

        assertThat(fileIO.batchSizes).isEmpty();
        assertThat(fileIO.singleDeletes).hasSize(5);
    }

    @Test
    void testTruncateDeletesFileByFile() throws Exception {
        BatchDeletingFileIO fileIO = new BatchDeletingFileIO(true);
        Path tablePath = new Path(tempDir.toUri());
        Path partitionPath = new Path(tablePath, PARTITION);
        writeOldFiles(fileIO, partitionPath, 5);
        newCommit(tablePath, fileIO, registeredPartition(), null, true, 8).truncateTable();

        assertThat(fileIO.batchSizes).isEmpty();
        assertThat(fileIO.singleDeletes).hasSize(5);
    }

    /** A catalog that has one registered partition, the one the old files sit in. */
    private static FormatTablePartitionManager registeredPartition() {
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        when(partitionManager.listPartitions(Collections.emptyMap(), null))
                .thenReturn(
                        Collections.singletonList(
                                new Partition(
                                        Collections.singletonMap("part", "2025"),
                                        0,
                                        0,
                                        0,
                                        0,
                                        1,
                                        true)));
        return partitionManager;
    }

    private FormatTableCommit newCommit(
            Path tablePath,
            FileIO fileIO,
            FormatTablePartitionManager partitionManager,
            Map<String, String> staticPartitions,
            boolean dynamicPartitionOverwrite,
            int cleanupThreadNum) {
        return new FormatTableCommit(
                tablePath.toString(),
                Collections.singletonList("part"),
                fileIO,
                false,
                PARTITION_DEFAULT_NAME.defaultValue(),
                true,
                Identifier.create("batch_db", "batch_table"),
                staticPartitions,
                null,
                null,
                partitionManager,
                dynamicPartitionOverwrite,
                cleanupThreadNum);
    }

    private static void commit(FormatTableCommit commit, FileIO fileIO, Path partitionPath)
            throws IOException {
        commit.commit(
                Collections.singletonList(new TwoPhaseCommitMessage(committer(partitionPath))));
    }

    private static TwoPhaseOutputStream.Committer committer(Path partitionPath) {
        TwoPhaseOutputStream.Committer committer = mock(TwoPhaseOutputStream.Committer.class);
        when(committer.targetPath()).thenReturn(new Path(partitionPath, "new-data.csv"));
        return committer;
    }

    private static void writeOldFiles(FileIO fileIO, Path partitionPath, int count)
            throws IOException {
        for (int i = 0; i < count; i++) {
            fileIO.writeFile(new Path(partitionPath, String.format("old-%04d.csv", i)), "1", false);
        }
    }

    private static int oldFilesLeft(FileIO fileIO, Path partitionPath) throws IOException {
        int left = 0;
        for (org.apache.paimon.fs.FileStatus status : fileIO.listStatus(partitionPath)) {
            if (status.getPath().getName().startsWith("old-")) {
                left++;
            }
        }
        return left;
    }

    /** A local file system that can delete whole batches and records how it was asked to. */
    private static class BatchDeletingFileIO extends LocalFileIO {

        private static final long serialVersionUID = 1L;

        private final boolean deletesInBatches;
        private final List<Integer> batchSizes = Collections.synchronizedList(new ArrayList<>());
        private final List<String> singleDeletes = Collections.synchronizedList(new ArrayList<>());

        /** The one-based batch this file system starts refusing at. */
        private int refuseBatchFrom = Integer.MAX_VALUE;

        private IOException failure;

        private BatchDeletingFileIO(boolean deletesInBatches) {
            this.deletesInBatches = deletesInBatches;
        }

        @Override
        public boolean deleteFilesInBatch(List<Path> files) throws IOException {
            batchSizes.add(files.size());
            if (failure != null) {
                throw failure;
            }
            if (!deletesInBatches || batchSizes.size() >= refuseBatchFrom) {
                return files.isEmpty();
            }
            for (Path file : files) {
                super.delete(file, false);
            }
            return true;
        }

        @Override
        public boolean delete(Path path, boolean recursive) throws IOException {
            // Rolling a failed commit back deletes the file it wrote, which is not cleanup.
            if (path.getName().startsWith("old-")) {
                singleDeletes.add(path.getName());
            }
            return super.delete(path, recursive);
        }
    }
}
