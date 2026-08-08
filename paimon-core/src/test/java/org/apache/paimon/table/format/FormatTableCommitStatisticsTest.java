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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.RenamingTwoPhaseOutputStream;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.paimon.CoreOptions.PARTITION_DEFAULT_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/** Tests for the partition statistics a {@link FormatTableCommit} reports. */
class FormatTableCommitStatisticsTest {

    private static final List<String> PARTITION_KEYS = Arrays.asList("year", "month");
    private static final String DEFAULT_PART_NAME = PARTITION_DEFAULT_NAME.defaultValue();
    private static final Identifier TABLE =
            Identifier.create("statistics_db", "statistics_format_table");

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testAppendReportsWhatItWroteAsAnIncrement() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        CommitMessage message = writtenFile(fileIO, tablePath, "year=2025/month=10", 3, 128);

        long before = System.currentTimeMillis();
        commit(tablePath, fileIO, partitionManager, false, null)
                .commit(Collections.singletonList(message));
        long after = System.currentTimeMillis();

        Reported reported = capture(partitionManager);
        assertThat(reported.replaceStatistics).isFalse();
        assertThat(reported.specs).containsExactly(spec("2025", "10"));
        assertThat(reported.statistics).hasSize(1);
        PartitionStatistics statistics = reported.statistics.get(0);
        assertThat(statistics.spec()).isEqualTo(spec("2025", "10"));
        assertThat(statistics.recordCount()).isEqualTo(3);
        assertThat(statistics.fileSizeInBytes()).isEqualTo(128);
        assertThat(statistics.fileCount()).isEqualTo(1);
        // The contract is the commit's wall clock, so bounds pin it where positivity cannot.
        assertThat(statistics.lastFileCreationTime()).isBetween(before, after);
        assertThat(statistics.totalBuckets()).isEqualTo(PartitionStatistics.UNKNOWN_TOTAL_BUCKETS);
    }

    @Test
    void testFilesOfOnePartitionAreSummed() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        List<CommitMessage> messages =
                Arrays.asList(
                        writtenFile(fileIO, tablePath, "year=2025/month=10", 3, 128),
                        writtenFile(fileIO, tablePath, "year=2025/month=10", 4, 256),
                        writtenFile(fileIO, tablePath, "year=2025/month=11", 5, 512));

        commit(tablePath, fileIO, partitionManager, false, null).commit(messages);

        Reported reported = capture(partitionManager);
        assertThat(reported.statistics)
                .hasSize(2)
                .anySatisfy(
                        statistics -> {
                            assertThat(statistics.spec()).isEqualTo(spec("2025", "10"));
                            assertThat(statistics.recordCount()).isEqualTo(7);
                            assertThat(statistics.fileSizeInBytes()).isEqualTo(384);
                            assertThat(statistics.fileCount()).isEqualTo(2);
                        })
                .anySatisfy(
                        statistics -> {
                            assertThat(statistics.spec()).isEqualTo(spec("2025", "11"));
                            assertThat(statistics.recordCount()).isEqualTo(5);
                            assertThat(statistics.fileCount()).isEqualTo(1);
                        });
    }

    @Test
    void testAFileNobodyCountedMakesThePartitionUnknown() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        List<CommitMessage> messages =
                Arrays.asList(
                        writtenFile(fileIO, tablePath, "year=2025/month=10", 3, 128),
                        // An older writer produced this one and counted nothing.
                        uncountedFile(fileIO, tablePath, "year=2025/month=10"),
                        // Counted, and after the one that was not: unknown has to stay unknown,
                        // or a partition missing a file comes out as an exact count of the rest.
                        writtenFile(fileIO, tablePath, "year=2025/month=10", 5, 512));

        commit(tablePath, fileIO, partitionManager, false, null).commit(messages);

        PartitionStatistics statistics = capture(partitionManager).statistics.get(0);
        // A sum missing a file must not be presented as an exact count.
        assertThat(statistics.recordCount()).isEqualTo(PartitionStatistics.UNKNOWN);
        assertThat(statistics.fileSizeInBytes()).isEqualTo(PartitionStatistics.UNKNOWN);
        // The file count is still exact: it is counted here, not reported by the writer.
        assertThat(statistics.fileCount()).isEqualTo(3);
    }

    @Test
    void testDynamicOverwriteReportsTheWholePartition() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        // Something was there before this commit replaced it.
        writeDataFile(fileIO, tablePath, "year=2025/month=10", "old-data.csv", 4096);
        CommitMessage message = writtenFile(fileIO, tablePath, "year=2025/month=10", 3, 128);

        commit(tablePath, fileIO, partitionManager, true, null)
                .commit(Collections.singletonList(message));

        Reported reported = capture(partitionManager);
        assertThat(reported.replaceStatistics).isTrue();
        assertThat(reported.statistics).hasSize(1);
        PartitionStatistics statistics = reported.statistics.get(0);
        assertThat(statistics.recordCount()).isEqualTo(3);
        assertThat(statistics.fileSizeInBytes()).isEqualTo(128);
        assertThat(statistics.fileCount()).isEqualTo(1);
    }

    @Test
    void testStaticPrefixOverwriteZeroesAClearedPartitionAndKeepsItRegistered() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        // Two sibling partitions hold data; the overwrite writes only one of them.
        writeDataFile(fileIO, tablePath, "year=2025/month=10", "old-data.csv", 4096);
        writeDataFile(fileIO, tablePath, "year=2025/month=11", "old-data.csv", 2048);
        CommitMessage message = writtenFile(fileIO, tablePath, "year=2025/month=10", 3, 128);

        long before = System.currentTimeMillis();
        commit(tablePath, fileIO, partitionManager, true, Collections.singletonMap("year", "2025"))
                .commit(Collections.singletonList(message));
        long after = System.currentTimeMillis();

        Reported reported = capture(partitionManager);
        long commitTime =
                reported.statistics.stream()
                        .filter(s -> s.spec().equals(spec("2025", "10")))
                        .findFirst()
                        .orElseThrow(AssertionError::new)
                        .lastFileCreationTime();
        assertThat(commitTime).isBetween(before, after);
        assertThat(reported.replaceStatistics).isTrue();
        // Red line: emptying a partition zeroes its statistics, it never unregisters it.
        assertThat(reported.specs)
                .containsExactlyInAnyOrder(spec("2025", "10"), spec("2025", "11"));
        verify(partitionManager, never()).dropPartitions(anyList());
        assertThat(reported.statistics)
                .anySatisfy(
                        statistics -> {
                            assertThat(statistics.spec()).isEqualTo(spec("2025", "11"));
                            assertThat(statistics.recordCount()).isZero();
                            assertThat(statistics.fileSizeInBytes()).isZero();
                            assertThat(statistics.fileCount()).isZero();
                            // Emptying is dated to the commit that did it. Reporting the time as
                            // unknown would leave the stored one describing files that are gone,
                            // since an unknown replaces nothing.
                            assertThat(statistics.lastFileCreationTime()).isEqualTo(commitTime);
                        });
    }

    @Test
    void testAClearedPartitionIsFoundEvenWhenTheListingAnswersUnderAnotherScheme()
            throws Exception {
        RescopingFileIO fileIO = new RescopingFileIO();
        Path tablePath = new Path(tempDir.toUri());
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        writeDataFile(fileIO, tablePath, "year=2025/month=10", "old-data.csv", 4096);
        writeDataFile(fileIO, tablePath, "year=2025/month=11", "old-data.csv", 2048);
        CommitMessage message = writtenFile(fileIO, tablePath, "year=2025/month=10", 3, 128);

        commit(tablePath, fileIO, partitionManager, true, Collections.singletonMap("year", "2025"))
                .commit(Collections.singletonList(message));

        // A listing does not have to answer under the URI it was asked with, and matching whole
        // paths would then throw away a directory this very listing produced — leaving an emptied
        // partition holding stale statistics.
        assertThat(capture(partitionManager).statistics)
                .anySatisfy(
                        statistics -> {
                            assertThat(statistics.spec()).isEqualTo(spec("2025", "11"));
                            assertThat(statistics.recordCount()).isZero();
                            assertThat(statistics.fileCount()).isZero();
                        });
    }

    @Test
    void testADirectoryThatIsNoPartitionOfThisTableIsNotReported() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        // The key=value layout, where a directory that is not a partition of this table has no
        // spec at all rather than a plausible wrong one: the prefix directory itself, and a
        // directory nested below a partition. Clearing the prefix deletes the files in both,
        // because the listing collects data files at every level, not only the partition one.
        writeDataFile(fileIO, tablePath, "year=2025/month=11", "old-data.csv", 2048);
        writeDataFile(fileIO, tablePath, "year=2025", "orphan.csv", 512);
        writeDataFile(fileIO, tablePath, "year=2025/month=10/nested", "old-data.csv", 1024);
        CommitMessage message = writtenFile(fileIO, tablePath, "year=2025/month=10", 3, 128);

        commit(tablePath, fileIO, partitionManager, true, Collections.singletonMap("year", "2025"))
                .commit(Collections.singletonList(message));

        // The commit succeeds and reports only the two real partitions. A directory with no spec
        // is left alone: its statistics go stale, which beats failing the commit that just wrote
        // the data, or accounting the files to a partition that does not exist.
        Reported reported = capture(partitionManager);
        assertThat(reported.specs)
                .containsExactlyInAnyOrder(spec("2025", "10"), spec("2025", "11"));
        assertThat(reported.statistics).hasSize(2);
    }

    @Test
    void testADirectoryBelowThePartitionIsNotReadAsAPartitionOfItsOwn() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        // In the value-only layout a partition directory is the bare value, so the trailing two
        // components of 2025/10/nested read as the plausible partition {year=10, month=nested}.
        writeDataFile(fileIO, tablePath, "2025/10", "old-data.csv", 4096);
        writeDataFile(fileIO, tablePath, "2025/10/nested", "old-data.csv", 4096);
        writeDataFile(fileIO, tablePath, "2025/11", "old-data.csv", 2048);
        CommitMessage message = writtenFile(fileIO, tablePath, "2025/10", 3, 128);

        commit(
                        tablePath,
                        fileIO,
                        partitionManager,
                        true,
                        Collections.singletonMap("year", "2025"),
                        true)
                .commit(Collections.singletonList(message));

        // Only directories the spec rebuilds are reported: accounting 2025/10/nested to a partition
        // named {year=10, month=nested} would zero a partition this commit never touched.
        Reported reported = capture(partitionManager);
        assertThat(reported.specs)
                .containsExactlyInAnyOrder(spec("2025", "10"), spec("2025", "11"));
        assertThat(reported.statistics)
                .hasSize(2)
                .anySatisfy(
                        statistics -> {
                            assertThat(statistics.spec()).isEqualTo(spec("2025", "10"));
                            assertThat(statistics.recordCount()).isEqualTo(3);
                            assertThat(statistics.fileCount()).isEqualTo(1);
                        })
                .anySatisfy(
                        statistics -> {
                            assertThat(statistics.spec()).isEqualTo(spec("2025", "11"));
                            assertThat(statistics.recordCount()).isZero();
                            assertThat(statistics.fileCount()).isZero();
                        });
    }

    @Test
    void testTheIncrementsOfConcurrentWritersOfOnePartitionSum() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        // A Flink sink commits once per writer subtask, in the subtask's own close(): writing one
        // partition at parallelism 3 is three independent commits against the same catalog, and
        // the partition total is whatever the catalog makes of the three reports.
        AccumulatingPartitionManager catalog = new AccumulatingPartitionManager();
        long[][] perSubtask = {{3, 128}, {4, 256}, {5, 512}};

        for (long[] subtask : perSubtask) {
            commit(tablePath, fileIO, catalog, false, null)
                    .commit(
                            Collections.singletonList(
                                    writtenFile(
                                            fileIO,
                                            tablePath,
                                            "year=2025/month=10",
                                            subtask[0],
                                            subtask[1])));
        }

        // Each subtask saw only its own files, so each reports an increment. Reporting the whole
        // partition instead would make the last subtask to close the only one that counted.
        assertThat(catalog.replaceFlags).containsExactly(false, false, false);
        assertThat(catalog.registered).containsOnly(spec("2025", "10"));
        PartitionStatistics total = catalog.stored.get(spec("2025", "10"));
        assertThat(total).isNotNull();
        assertThat(total.recordCount()).isEqualTo(12);
        assertThat(total.fileSizeInBytes()).isEqualTo(896);
        assertThat(total.fileCount()).isEqualTo(3);
    }

    private FormatTableCommit commit(
            Path tablePath,
            FileIO fileIO,
            FormatTablePartitionManager partitionManager,
            boolean overwrite,
            Map<String, String> staticPartitions) {
        return commit(tablePath, fileIO, partitionManager, overwrite, staticPartitions, false);
    }

    private FormatTableCommit commit(
            Path tablePath,
            FileIO fileIO,
            FormatTablePartitionManager partitionManager,
            boolean overwrite,
            Map<String, String> staticPartitions,
            boolean onlyValueInPath) {
        return new FormatTableCommit(
                tablePath.toString(),
                PARTITION_KEYS,
                fileIO,
                onlyValueInPath,
                DEFAULT_PART_NAME,
                overwrite,
                TABLE,
                staticPartitions,
                null,
                null,
                partitionManager);
    }

    /** A file this commit wrote, with the counts its writer took. */
    private CommitMessage writtenFile(
            FileIO fileIO,
            Path tablePath,
            String partitionDir,
            long recordCount,
            long fileSizeInBytes)
            throws Exception {
        return new TwoPhaseCommitMessage(
                stage(fileIO, tablePath, partitionDir), recordCount, fileSizeInBytes);
    }

    /** A file committed by a writer that reported no counts. */
    private CommitMessage uncountedFile(LocalFileIO fileIO, Path tablePath, String partitionDir)
            throws Exception {
        return new TwoPhaseCommitMessage(stage(fileIO, tablePath, partitionDir));
    }

    private TwoPhaseOutputStream.Committer stage(FileIO fileIO, Path tablePath, String partitionDir)
            throws Exception {
        Path targetPath =
                new Path(new Path(tablePath, partitionDir), "data-" + UUID.randomUUID() + ".csv");
        RenamingTwoPhaseOutputStream outputStream =
                new RenamingTwoPhaseOutputStream(fileIO, targetPath, false);
        outputStream.write(1);
        return outputStream.closeForCommit();
    }

    private static void writeDataFile(
            FileIO fileIO, Path tablePath, String partitionDir, String name, int bytes)
            throws Exception {
        Path path = new Path(new Path(tablePath, partitionDir), name);
        fileIO.mkdirs(path.getParent());
        try (PositionOutputStream out = fileIO.newOutputStream(path, false)) {
            out.write(new byte[bytes]);
        }
    }

    private static Map<String, String> spec(String year, String month) {
        LinkedHashMap<String, String> spec = new LinkedHashMap<>();
        spec.put("year", year);
        spec.put("month", month);
        return spec;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Reported capture(FormatTablePartitionManager partitionManager) {
        ArgumentCaptor<List<Map<String, String>>> specs =
                ArgumentCaptor.forClass((Class) List.class);
        ArgumentCaptor<List<PartitionStatistics>> statistics =
                ArgumentCaptor.forClass((Class) List.class);
        ArgumentCaptor<Boolean> replaceStatistics = ArgumentCaptor.forClass(Boolean.class);
        verify(partitionManager)
                .createPartitions(
                        specs.capture(),
                        eq(true),
                        statistics.capture(),
                        replaceStatistics.capture());
        return new Reported(
                new ArrayList<>(specs.getValue()),
                new ArrayList<>(statistics.getValue()),
                replaceStatistics.getValue());
    }

    /**
     * A {@link FileIO} that answers a listing with paths stripped of their scheme, the way a
     * delegating one does when it resolves the caller's scheme to the one it really uses.
     */
    private static class RescopingFileIO extends LocalFileIO {

        private static final long serialVersionUID = 1L;

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            FileStatus[] statuses = super.listStatus(path);
            for (int i = 0; i < statuses.length; i++) {
                statuses[i] = new RescopedFileStatus(statuses[i]);
            }
            return statuses;
        }
    }

    private static class RescopedFileStatus implements FileStatus {

        private final FileStatus delegate;

        private RescopedFileStatus(FileStatus delegate) {
            this.delegate = delegate;
        }

        @Override
        public long getLen() {
            return delegate.getLen();
        }

        @Override
        public boolean isDir() {
            return delegate.isDir();
        }

        @Override
        public Path getPath() {
            return new Path(delegate.getPath().toUri().getPath());
        }

        @Override
        public long getModificationTime() {
            return delegate.getModificationTime();
        }
    }

    /**
     * A partition manager that folds the reports it receives the way a catalog does: ADD
     * accumulates onto what is held, SET replaces it. It holds what several independent commits
     * against one table add up to.
     */
    private static class AccumulatingPartitionManager implements FormatTablePartitionManager {

        private static final long serialVersionUID = 1L;

        private final List<Map<String, String>> registered = new ArrayList<>();
        private final Map<Map<String, String>, PartitionStatistics> stored = new LinkedHashMap<>();
        private final List<Boolean> replaceFlags = new ArrayList<>();

        @Override
        public void createPartitions(
                List<Map<String, String>> partitions,
                boolean ignoreIfExists,
                @Nullable List<PartitionStatistics> statistics,
                boolean replaceStatistics) {
            createPartitions(partitions, ignoreIfExists);
            if (statistics == null) {
                return;
            }
            replaceFlags.add(replaceStatistics);
            for (PartitionStatistics reported : statistics) {
                PartitionStatistics held = stored.get(reported.spec());
                if (held == null || replaceStatistics) {
                    stored.put(reported.spec(), reported);
                    continue;
                }
                stored.put(
                        reported.spec(),
                        new PartitionStatistics(
                                reported.spec(),
                                held.recordCount() + reported.recordCount(),
                                held.fileSizeInBytes() + reported.fileSizeInBytes(),
                                held.fileCount() + reported.fileCount(),
                                Math.max(
                                        held.lastFileCreationTime(),
                                        reported.lastFileCreationTime()),
                                PartitionStatistics.UNKNOWN_TOTAL_BUCKETS));
            }
        }

        @Override
        public void createPartitions(List<Map<String, String>> partitions, boolean ignoreIfExists) {
            registered.addAll(partitions);
        }

        @Override
        public List<Partition> listPartitions(
                Map<String, String> prefix, @Nullable Predicate filter) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Partition> listPartitionsByNames(List<Map<String, String>> partitions) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropPartitions(List<Map<String, String>> partitions) {
            throw new UnsupportedOperationException();
        }
    }

    @Test
    void testTheNumbersReachTheCatalogThroughTheWriteBuilder() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.PATH.key(), tablePath.toString());
        options.put(CoreOptions.FILE_FORMAT.key(), "csv");
        FormatTable table =
                FormatTable.builder()
                        .fileIO(fileIO)
                        .identifier(Identifier.create("test_db", "test_table"))
                        .rowType(
                                RowType.of(
                                        new DataType[] {DataTypes.INT(), DataTypes.STRING()},
                                        new String[] {"id", "year"}))
                        .partitionKeys(Collections.singletonList("year"))
                        .location(tablePath.toString())
                        .format(FormatTable.Format.CSV)
                        .options(options)
                        .partitionManager(partitionManager)
                        .build();

        // The whole path rather than a commit built by hand: the write builder, the commit it
        // builds, and the numbers the writer counted on the way.
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        BatchTableWrite write = writeBuilder.newWrite();
        write.write(GenericRow.of(1, BinaryString.fromString("2025")));
        write.write(GenericRow.of(2, BinaryString.fromString("2025")));
        List<CommitMessage> messages = write.prepareCommit();
        writeBuilder.newCommit().commit(messages);

        Reported reported = capture(partitionManager);
        assertThat(reported.replaceStatistics).isFalse();
        assertThat(reported.specs).containsExactly(Collections.singletonMap("year", "2025"));
        assertThat(reported.statistics).hasSize(1);
        PartitionStatistics statistics = reported.statistics.get(0);
        assertThat(statistics.recordCount()).isEqualTo(2);
        assertThat(statistics.fileCount()).isEqualTo(1);
        // The byte size is the writer's own count, so it has to match what landed on disk.
        long onDisk = 0;
        for (FileStatus file : fileIO.listStatus(new Path(tablePath, "year=2025"))) {
            if (!file.isDir()) {
                onDisk += file.getLen();
            }
        }
        assertThat(statistics.fileSizeInBytes()).isEqualTo(onDisk);
    }

    @Test
    void testAFailedReportFailsTheCommitAndDiscardsWhatItWrote() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        RuntimeException failure = new RuntimeException("catalog says 429");
        doThrow(failure)
                .when(partitionManager)
                .createPartitions(anyList(), anyBoolean(), any(), anyBoolean());
        CommitMessage message = writtenFile(fileIO, tablePath, "year=2025/month=10", 3, 128);
        Path written = ((TwoPhaseCommitMessage) message).getCommitter().targetPath();

        // Registration and statistics ride in one request, so a failure says nothing about
        // whether the partition was registered. Committing anyway would leave data behind that
        // nothing points at.
        assertThatThrownBy(
                        () ->
                                commit(tablePath, fileIO, partitionManager, false, null)
                                        .commit(Collections.singletonList(message)))
                .hasRootCause(failure);

        assertThat(fileIO.exists(written)).isFalse();
    }

    @Test
    void testAFailedReportOfAnOverwriteLeavesThePartitionEmpty() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        doThrow(new RuntimeException("catalog says 429"))
                .when(partitionManager)
                .createPartitions(anyList(), anyBoolean(), any(), anyBoolean());
        writeDataFile(fileIO, tablePath, "year=2025/month=10", "old-data.csv", 4096);
        CommitMessage message = writtenFile(fileIO, tablePath, "year=2025/month=10", 3, 128);
        Path written = ((TwoPhaseCommitMessage) message).getCommitter().targetPath();

        assertThatThrownBy(
                        () ->
                                commit(
                                                tablePath,
                                                fileIO,
                                                partitionManager,
                                                true,
                                                Collections.singletonMap("year", "2025"))
                                        .commit(Collections.singletonList(message)))
                .hasRootCauseMessage("catalog says 429");

        // The state this leaves is worth stating rather than discovering: the overwrite already
        // deleted what the partition held, and the abort takes back what it wrote, so the
        // partition is empty on disk while the catalog still describes what used to be there.
        assertThat(fileIO.exists(written)).isFalse();
        assertThat(fileIO.exists(new Path(tablePath, "year=2025/month=10/old-data.csv"))).isFalse();
    }

    /** What one call reported to the catalog. */
    private static class Reported {
        private final List<Map<String, String>> specs;
        private final List<PartitionStatistics> statistics;
        private final boolean replaceStatistics;

        private Reported(
                List<Map<String, String>> specs,
                List<PartitionStatistics> statistics,
                boolean replaceStatistics) {
            this.specs = specs;
            this.statistics = statistics;
            this.replaceStatistics = replaceStatistics;
        }
    }
}
