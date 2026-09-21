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
import org.apache.paimon.PagedList;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
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
import org.apache.paimon.utils.InstantiationUtil;

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
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.CoreOptions.PARTITION_DEFAULT_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

        Reported reported = captureAndValidateReport(partitionManager, tablePath);
        assertThat(reported.replaceStatistics).isFalse();
        assertThat(reported.partitionOptions).isNull();
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

        Reported reported = captureAndValidateReport(partitionManager, tablePath);
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

        PartitionStatistics statistics =
                captureAndValidateReport(partitionManager, tablePath).statistics.get(0);
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
        Path oldWrittenPartitionData = new Path(tablePath, "year=2025/month=10/old-data.csv");
        writeDataFile(fileIO, tablePath, "year=2025/month=10", "old-data.csv", 4096);
        CommitMessage message = writtenFile(fileIO, tablePath, "year=2025/month=10", 3, 128);

        commit(tablePath, fileIO, partitionManager, true, null)
                .commit(Collections.singletonList(message));

        Reported reported = captureAndValidateReport(partitionManager, tablePath);
        assertThat(reported.replaceStatistics).isTrue();
        assertThat(reported.partitionOptions)
                .containsExactly(returnsToDefault(tablePath, spec("2025", "10")));
        assertThat(fileIO.exists(oldWrittenPartitionData)).isFalse();
        verify(partitionManager, never()).listPartitions(any(), isNull());
        verify(partitionManager, never()).listPartitionsByNames(anyList());
        assertThat(reported.statistics).hasSize(1);
        PartitionStatistics statistics = reported.statistics.get(0);
        assertThat(statistics.recordCount()).isEqualTo(3);
        assertThat(statistics.fileSizeInBytes()).isEqualTo(128);
        assertThat(statistics.fileCount()).isEqualTo(1);
    }

    @Test
    void testEmptyStaticOverwriteReportsZeroAndResetsLocationWithoutRegistryRead()
            throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        Map<String, String> target = spec("2025", "10");

        commit(tablePath, fileIO, partitionManager, true, target).commit(Collections.emptyList());

        Reported reported = captureAndValidateReport(partitionManager, tablePath);
        assertThat(reported.specs).containsExactly(target);
        assertThat(reported.statistics).hasSize(1);
        assertThat(reported.statistics.get(0).spec()).isEqualTo(target);
        assertThat(reported.statistics.get(0).recordCount()).isZero();
        assertThat(reported.statistics.get(0).fileSizeInBytes()).isZero();
        assertThat(reported.statistics.get(0).fileCount()).isZero();
        assertThat(reported.replaceStatistics).isTrue();
        assertThat(reported.partitionOptions)
                .containsExactly(returnsToDefault(tablePath, spec("2025", "10")));
        verify(partitionManager, never()).listPartitions(any(), isNull());
        verify(partitionManager, never()).listPartitionsByNames(anyList());
    }

    @Test
    void testStaticPrefixOverwriteReportsRegistryAndWrittenTargets() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        Map<String, String> prefix = Collections.singletonMap("year", "2025");
        // The catalog has one unwritten partition with data and one whose directory is missing.
        // The written partition is not registered yet, so the target set has to include both the
        // prefix lookup and the commit messages.
        registered(partitionManager, spec("2025", "11"), spec("2025", "12"), spec("2024", "09"));
        Path oldWrittenPartitionData = new Path(tablePath, "year=2025/month=10/old-data.csv");
        writeDataFile(fileIO, tablePath, "year=2025/month=10", "old-data.csv", 4096);
        writeDataFile(fileIO, tablePath, "year=2025/month=11", "old-data.csv", 2048);
        CommitMessage message = writtenFile(fileIO, tablePath, "year=2025/month=10", 3, 128);

        long before = System.currentTimeMillis();
        commit(tablePath, fileIO, partitionManager, true, prefix)
                .commit(Collections.singletonList(message));
        long after = System.currentTimeMillis();

        Reported reported = captureAndValidateReport(partitionManager, tablePath);
        long commitTime =
                reported.statistics.stream()
                        .filter(s -> s.spec().equals(spec("2025", "10")))
                        .findFirst()
                        .orElseThrow(AssertionError::new)
                        .lastFileCreationTime();
        assertThat(commitTime).isBetween(before, after);
        assertThat(reported.replaceStatistics).isTrue();
        assertThat(fileIO.exists(oldWrittenPartitionData)).isFalse();
        // Red line: emptying a partition zeroes its statistics, it never unregisters it.
        assertThat(reported.specs)
                .containsExactlyInAnyOrder(
                        spec("2025", "10"), spec("2025", "11"), spec("2025", "12"));
        verify(partitionManager).listPartitions(prefix, null);
        verify(partitionManager, never()).listPartitionsByNames(anyList());
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
                        })
                .anySatisfy(
                        statistics -> {
                            assertThat(statistics.spec()).isEqualTo(spec("2025", "12"));
                            assertThat(statistics.recordCount()).isZero();
                            assertThat(statistics.fileSizeInBytes()).isZero();
                            assertThat(statistics.fileCount()).isZero();
                            assertThat(statistics.lastFileCreationTime()).isEqualTo(commitTime);
                        });
    }

    @Test
    void testOverwritingTheWholeTableZeroesAPartitionItDidNotRewrite() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        registered(partitionManager, spec("2025", "10"), spec("2025", "11"));
        writeDataFile(fileIO, tablePath, "year=2025/month=10", "old-data.csv", 4096);
        writeDataFile(fileIO, tablePath, "year=2025/month=11", "old-data.csv", 2048);
        // The statement names no partition, and its query wrote only one of the two.
        CommitMessage message = writtenFile(fileIO, tablePath, "year=2025/month=10", 3, 128);

        overwritingTheWholeTable(tablePath, fileIO, partitionManager)
                .commit(Collections.singletonList(message));

        Reported reported = captureAndValidateReport(partitionManager, tablePath);
        assertThat(reported.replaceStatistics).isTrue();
        assertThat(reported.specs)
                .containsExactlyInAnyOrder(spec("2025", "10"), spec("2025", "11"));
        assertThat(reported.statistics)
                .anySatisfy(
                        statistics -> {
                            assertThat(statistics.spec()).isEqualTo(spec("2025", "10"));
                            assertThat(statistics.recordCount()).isEqualTo(3);
                            assertThat(statistics.fileSizeInBytes()).isEqualTo(128);
                            assertThat(statistics.fileCount()).isEqualTo(1);
                        })
                // Emptied and not written to: the whole table was replaced, so an exact zero
                // rather than the numbers of the files this commit deleted.
                .anySatisfy(
                        statistics -> {
                            assertThat(statistics.spec()).isEqualTo(spec("2025", "11"));
                            assertThat(statistics.recordCount()).isZero();
                            assertThat(statistics.fileSizeInBytes()).isZero();
                            assertThat(statistics.fileCount()).isZero();
                        });
        verify(partitionManager).listPartitions(Collections.emptyMap(), null);
        verify(partitionManager, never()).listPartitionsByNames(anyList());
    }

    @Test
    void testOverwritingTheWholeTableLeavesADirectoryTheCatalogHasNotRegistered() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        registered(partitionManager, spec("2025", "10"));
        writeDataFile(fileIO, tablePath, "year=2025/month=10", "data.csv", 4096);
        // A directory MSCK REPAIR TABLE has not registered yet: no scan of this table reads it,
        // so replacing what the table holds is not this directory's business either.
        writeDataFile(fileIO, tablePath, "year=2025/month=11", "unregistered.csv", 2048);

        overwritingTheWholeTable(tablePath, fileIO, partitionManager)
                .commit(Collections.emptyList());

        assertThat(fileIO.exists(new Path(tablePath, "year=2025/month=10/data.csv"))).isFalse();
        assertThat(fileIO.exists(new Path(tablePath, "year=2025/month=11/unregistered.csv")))
                .isTrue();
        // Nor does the overwrite register it: reporting a zero for it would make a partition the
        // catalog never had, out of a directory that still holds rows.
        Reported reported = captureAndValidateReport(partitionManager, tablePath);
        assertThat(reported.specs).containsExactly(spec("2025", "10"));
        verify(partitionManager).listPartitions(Collections.emptyMap(), null);
        verify(partitionManager, never()).listPartitionsByNames(anyList());
    }

    @Test
    void testTruncatingPartitionsReportsAnExactZeroAsTheTotal() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        registered(partitionManager, spec("2025", "10"), spec("2025", "11"), spec("2024", "12"));
        writeDataFile(fileIO, tablePath, "year=2025/month=10", "data.csv", 4096);
        writeDataFile(fileIO, tablePath, "year=2025/month=11", "data.csv", 2048);
        writeDataFile(fileIO, tablePath, "year=2024/month=12", "data.csv", 1024);

        long before = System.currentTimeMillis();
        commit(tablePath, fileIO, partitionManager, false, null)
                .truncatePartitions(Arrays.asList(spec("2025", "10"), spec("2025", "11")));
        long after = System.currentTimeMillis();

        Reported reported = captureAndValidateReport(partitionManager, tablePath);
        // What a truncated partition holds is zero, not zero fewer rows than before.
        assertThat(reported.replaceStatistics).isTrue();
        assertThat(reported.specs)
                .containsExactlyInAnyOrder(spec("2025", "10"), spec("2025", "11"));
        // Red line: emptying a partition zeroes its statistics, it never unregisters it.
        verify(partitionManager, never()).dropPartitions(anyList());
        // Complete names are resolved directly; truncating them never loads the full registry.
        verify(partitionManager)
                .listPartitionsByNames(Arrays.asList(spec("2025", "10"), spec("2025", "11")));
        verify(partitionManager, never()).listPartitions(any(), isNull());
        assertThat(fileIO.exists(new Path(tablePath, "year=2024/month=12/data.csv"))).isTrue();
        // Statistics route by the spec they carry, so every partition needs its own.
        assertThat(reported.statistics)
                .extracting(PartitionStatistics::spec)
                .containsExactlyInAnyOrder(spec("2025", "10"), spec("2025", "11"));
        assertThat(reported.statistics)
                .allSatisfy(
                        statistics -> {
                            assertThat(statistics.recordCount()).isZero();
                            assertThat(statistics.fileSizeInBytes()).isZero();
                            assertThat(statistics.fileCount()).isZero();
                            // Dated to the truncation. Reporting the time as unknown would leave
                            // the stored one describing files that are gone, since an unknown
                            // replaces nothing.
                            assertThat(statistics.lastFileCreationTime()).isBetween(before, after);
                        });
    }

    @Test
    void testTruncatingAPartitionThatIsAlreadyEmptyStillReportsZero() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        // Registered, and its directory holds nothing to delete - whoever emptied it, its stored
        // statistics still describe the files that used to be there.
        registered(partitionManager, spec("2025", "10"));
        fileIO.mkdirs(new Path(tablePath, "year=2025/month=10"));

        commit(tablePath, fileIO, partitionManager, false, null)
                .truncatePartitions(Collections.singletonList(spec("2025", "10")));

        Reported reported = captureAndValidateReport(partitionManager, tablePath);
        // A registered exact target remains a target even when it already holds no files.
        assertThat(reported.replaceStatistics).isTrue();
        assertThat(reported.specs).containsExactly(spec("2025", "10"));
        assertThat(reported.statistics).hasSize(1);
        assertThat(reported.statistics.get(0).spec()).isEqualTo(spec("2025", "10"));
        assertThat(reported.statistics.get(0).recordCount()).isZero();
        assertThat(reported.statistics.get(0).fileCount()).isZero();
        verify(partitionManager)
                .listPartitionsByNames(Collections.singletonList(spec("2025", "10")));
        verify(partitionManager, never()).listPartitions(any(), isNull());
    }

    @Test
    void testTruncatingTheTableReportsARegisteredPartitionWhoseDirectoryIsGone() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        registered(partitionManager, spec("2025", "10"), spec("2025", "11"));
        writeDataFile(fileIO, tablePath, "year=2025/month=10", "data.csv", 4096);
        // Registered but its directory is not there at all: someone deleted it behind the catalog.
        // A missing directory is drift to report on, not a reason to fail the truncation of the
        // partitions that do exist.

        commit(tablePath, fileIO, partitionManager, false, null).truncateTable();

        Reported reported = captureAndValidateReport(partitionManager, tablePath);
        assertThat(reported.replaceStatistics).isTrue();
        assertThat(reported.specs)
                .containsExactlyInAnyOrder(spec("2025", "10"), spec("2025", "11"));
        assertThat(reported.statistics)
                .allSatisfy(
                        statistics -> {
                            assertThat(statistics.recordCount()).isZero();
                            assertThat(statistics.fileCount()).isZero();
                        });
        verify(partitionManager).listPartitions(Collections.emptyMap(), null);
        verify(partitionManager, never()).listPartitionsByNames(anyList());
    }

    @Test
    void testTruncatingAPrefixReportsThePartitionsUnderneathIt() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        Map<String, String> prefix = Collections.singletonMap("year", "2025");
        registered(partitionManager, spec("2025", "10"), spec("2025", "11"), spec("2024", "12"));
        writeDataFile(fileIO, tablePath, "year=2025/month=10", "data.csv", 4096);
        writeDataFile(fileIO, tablePath, "year=2025/month=11", "data.csv", 2048);
        // Registered but outside the requested prefix.
        writeDataFile(fileIO, tablePath, "year=2024/month=12", "data.csv", 1024);
        // Under the prefix but not registered: a directory still waiting for MSCK REPAIR TABLE is
        // not a partition of the table, so truncating neither empties nor registers it.
        writeDataFile(fileIO, tablePath, "year=2025/month=12", "data.csv", 512);

        commit(tablePath, fileIO, partitionManager, false, null)
                .truncatePartitions(Collections.singletonList(prefix));

        Reported reported = captureAndValidateReport(partitionManager, tablePath);
        // The prefix names no partition of its own; the partitions it empties are the registered
        // ones underneath it.
        assertThat(reported.specs)
                .containsExactlyInAnyOrder(spec("2025", "10"), spec("2025", "11"));
        assertThat(reported.replaceStatistics).isTrue();
        assertThat(reported.statistics)
                .allSatisfy(statistics -> assertThat(statistics.recordCount()).isZero());
        assertThat(fileIO.exists(new Path(tablePath, "year=2025/month=12/data.csv"))).isTrue();
        assertThat(fileIO.exists(new Path(tablePath, "year=2024/month=12/data.csv"))).isTrue();
        verify(partitionManager).listPartitions(prefix, null);
        verify(partitionManager, never()).listPartitionsByNames(anyList());
    }

    @Test
    void testTruncatingTheTableReportsEveryRegisteredPartition() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        registered(partitionManager, spec("2025", "10"), spec("2025", "11"));
        writeDataFile(fileIO, tablePath, "year=2025/month=10", "data.csv", 4096);
        writeDataFile(fileIO, tablePath, "year=2025/month=11", "data.csv", 2048);
        // Not registered, so not part of the table and not reported on.
        writeDataFile(fileIO, tablePath, "year=2025/month=12", "data.csv", 512);

        commit(tablePath, fileIO, partitionManager, false, null).truncateTable();

        Reported reported = captureAndValidateReport(partitionManager, tablePath);
        assertThat(reported.replaceStatistics).isTrue();
        assertThat(reported.specs)
                .containsExactlyInAnyOrder(spec("2025", "10"), spec("2025", "11"));
        assertThat(reported.statistics)
                .allSatisfy(statistics -> assertThat(statistics.fileCount()).isZero());
        assertThat(fileIO.exists(new Path(tablePath, "year=2025/month=12/data.csv"))).isTrue();
        verify(partitionManager).listPartitions(Collections.emptyMap(), null);
        verify(partitionManager, never()).listPartitionsByNames(anyList());
    }

    @Test
    void testAFailedTruncationReportsThePartitionsItAlreadyEmptied() throws Exception {
        Path tablePath = new Path(tempDir.toUri());
        LocalFileIO fileIO = new UndeletableFileIO(new Path(tablePath, "year=2025/month=11"));
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        registered(partitionManager, spec("2025", "10"), spec("2025", "11"));
        writeDataFile(fileIO, tablePath, "year=2025/month=10", "data.csv", 4096);
        writeDataFile(fileIO, tablePath, "year=2025/month=11", "data.csv", 2048);

        assertThatThrownBy(
                        () ->
                                commit(tablePath, fileIO, partitionManager, false, null)
                                        .truncateTable())
                .hasMessageContaining("month=11")
                .hasMessageContaining(TABLE.getFullName());

        // A Format Table has no snapshot to make the whole truncation atomic, so what it emptied
        // before the failure is reported anyway: the catalog must not keep describing files that
        // are gone.
        Reported reported = captureAndValidateReport(partitionManager, tablePath);
        assertThat(reported.specs).containsExactly(spec("2025", "10"));
        assertThat(reported.statistics.get(0).fileCount()).isZero();
    }

    @Test
    void testATruncationFailsWhenADeletionIsRefused() throws Exception {
        Path tablePath = new Path(tempDir.toUri());
        LocalFileIO fileIO = new RefusingFileIO();
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        registered(partitionManager, spec("2025", "10"));
        writeDataFile(fileIO, tablePath, "year=2025/month=10", "data.csv", 4096);

        assertThatThrownBy(
                        () ->
                                commit(tablePath, fileIO, partitionManager, false, null)
                                        .truncateTable())
                .hasMessageContaining("month=10")
                .hasMessageContaining(TABLE.getFullName())
                .hasStackTraceContaining("data.csv");

        // A refused deletion is not a concurrent one: the rows are still readable, so reporting
        // the partition as holding nothing would hide them.
        verify(partitionManager, never())
                .createPartitions(anyList(), anyBoolean(), anyList(), anyBoolean(), any());
        assertThat(fileIO.exists(new Path(tablePath, "year=2025/month=10/data.csv"))).isTrue();
    }

    @Test
    void testAFailedReportDoesNotHideTheDeletionThatFailedFirst() throws Exception {
        Path tablePath = new Path(tempDir.toUri());
        LocalFileIO fileIO = new UndeletableFileIO(new Path(tablePath, "year=2025/month=11"));
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        registered(partitionManager, spec("2025", "10"), spec("2025", "11"));
        doThrow(new RuntimeException("the catalog is unreachable"))
                .when(partitionManager)
                .createPartitions(anyList(), anyBoolean(), anyList(), anyBoolean(), any());
        writeDataFile(fileIO, tablePath, "year=2025/month=10", "data.csv", 4096);
        writeDataFile(fileIO, tablePath, "year=2025/month=11", "data.csv", 2048);

        assertThatThrownBy(
                        () ->
                                commit(tablePath, fileIO, partitionManager, false, null)
                                        .truncateTable())
                // The deletion that failed first is what explains the failure; the report that
                // could not record the rest is attached to it.
                .hasMessageContaining("month=11")
                .satisfies(
                        thrown ->
                                assertThat(thrown.getSuppressed())
                                        .anySatisfy(
                                                suppressed ->
                                                        assertThat(suppressed)
                                                                .hasMessageContaining(
                                                                        "unreachable")));
    }

    @Test
    void testARegisteredTargetIsReportedWhenFileListingAnswersUnderAnotherScheme()
            throws Exception {
        RescopingFileIO fileIO = new RescopingFileIO();
        Path tablePath = new Path(tempDir.toUri());
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        registered(partitionManager, spec("2025", "11"));
        writeDataFile(fileIO, tablePath, "year=2025/month=10", "old-data.csv", 4096);
        writeDataFile(fileIO, tablePath, "year=2025/month=11", "old-data.csv", 2048);
        CommitMessage message = writtenFile(fileIO, tablePath, "year=2025/month=10", 3, 128);

        commit(tablePath, fileIO, partitionManager, true, Collections.singletonMap("year", "2025"))
                .commit(Collections.singletonList(message));

        // The registry, not a filesystem path parsed after deletion, defines month=11 as a target.
        // A listing may still answer under a different URI scheme, and deletion must handle that
        // without losing the target's replacement statistics.
        assertThat(captureAndValidateReport(partitionManager, tablePath).statistics)
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
        registered(partitionManager, spec("2025", "11"));
        // The registry contributes month=11 and the commit contributes month=10. A file directly
        // under the prefix is outside both complete partition targets; a nested directory is data
        // inside month=10, not a third partition inferred from its path.
        writeDataFile(fileIO, tablePath, "year=2025/month=11", "old-data.csv", 2048);
        writeDataFile(fileIO, tablePath, "year=2025", "orphan.csv", 512);
        writeDataFile(fileIO, tablePath, "year=2025/month=10/nested", "old-data.csv", 1024);
        CommitMessage message = writtenFile(fileIO, tablePath, "year=2025/month=10", 3, 128);

        commit(tablePath, fileIO, partitionManager, true, Collections.singletonMap("year", "2025"))
                .commit(Collections.singletonList(message));

        // The commit reports only the registry-defined and written targets. It leaves the foreign
        // prefix file alone and deletes the nested file as part of month=10 without inventing a
        // partition for either path.
        Reported reported = captureAndValidateReport(partitionManager, tablePath);
        assertThat(reported.specs)
                .containsExactlyInAnyOrder(spec("2025", "10"), spec("2025", "11"));
        assertThat(reported.statistics).hasSize(2);
        assertThat(fileIO.exists(new Path(tablePath, "year=2025/orphan.csv"))).isTrue();
        assertThat(fileIO.exists(new Path(tablePath, "year=2025/month=10/nested/old-data.csv")))
                .isFalse();
    }

    @Test
    void testADirectoryBelowThePartitionIsNotReadAsAPartitionOfItsOwn() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        registered(partitionManager, spec("2025", "11"));
        // In the value-only layout the trailing components of 2025/10/nested could look like the
        // plausible partition {year=10, month=nested}. Targets must still come only from the
        // registry and the commit's written specs.
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

        // The nested path is deleted as data under month=10, but is never reported as a partition.
        Reported reported = captureAndValidateReport(partitionManager, tablePath, true);
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
        return commit(
                tablePath,
                fileIO,
                partitionManager,
                overwrite,
                staticPartitions,
                onlyValueInPath,
                /* dynamicPartitionOverwrite */ true);
    }

    private FormatTableCommit commit(
            Path tablePath,
            FileIO fileIO,
            FormatTablePartitionManager partitionManager,
            boolean overwrite,
            Map<String, String> staticPartitions,
            boolean onlyValueInPath,
            boolean dynamicPartitionOverwrite) {
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
                partitionManager,
                dynamicPartitionOverwrite);
    }

    /** An overwrite that names no partition: INSERT OVERWRITE without a PARTITION clause. */
    private FormatTableCommit overwritingTheWholeTable(
            Path tablePath, FileIO fileIO, FormatTablePartitionManager partitionManager) {
        return commit(tablePath, fileIO, partitionManager, true, null, false, false);
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

    /** A partition the catalog holds, whose statistics the truncation is meant to replace. */
    private static Partition partition(Map<String, String> spec) {
        return new Partition(spec, 3, 4096, 1, 0, PartitionStatistics.UNKNOWN_TOTAL_BUCKETS, false);
    }

    /** Which partitions the catalog says the table has. */
    @SafeVarargs
    private static void registered(
            FormatTablePartitionManager partitionManager, Map<String, String>... specs) {
        List<Partition> partitions = new ArrayList<>();
        for (Map<String, String> spec : specs) {
            partitions.add(partition(spec));
        }
        when(partitionManager.listPartitions(any(), isNull()))
                .thenAnswer(
                        invocation -> {
                            Map<String, String> prefix = invocation.getArgument(0);
                            List<Partition> found = new ArrayList<>();
                            for (Partition partition : partitions) {
                                if (partition.spec().entrySet().containsAll(prefix.entrySet())) {
                                    found.add(partition);
                                }
                            }
                            return found;
                        });
        when(partitionManager.listPartitionsByNames(anyList()))
                .thenAnswer(
                        invocation -> {
                            List<Map<String, String>> asked = invocation.getArgument(0);
                            List<Partition> found = new ArrayList<>();
                            for (Partition partition : partitions) {
                                if (asked.contains(partition.spec())) {
                                    found.add(partition);
                                }
                            }
                            return found;
                        });
    }

    private static Map<String, String> spec(String year, String month) {
        LinkedHashMap<String, String> spec = new LinkedHashMap<>();
        spec.put("year", year);
        spec.put("month", month);
        return spec;
    }

    /** What a replacement sends for a partition: the directory the partition belongs in. */
    private static Map<String, String> returnsToDefault(Path tablePath, Map<String, String> spec) {
        return returnsToDefault(tablePath, spec, false);
    }

    private static Map<String, String> returnsToDefault(
            Path tablePath, Map<String, String> spec, boolean onlyValueInPath) {
        return FormatTableCommitTest.defaultDirectoryOption(tablePath, spec, onlyValueInPath);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Reported captureAndValidateReport(
            FormatTablePartitionManager partitionManager, Path tablePath) {
        return captureAndValidateReport(partitionManager, tablePath, false);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Reported captureAndValidateReport(
            FormatTablePartitionManager partitionManager, Path tablePath, boolean onlyValueInPath) {
        ArgumentCaptor<List<Map<String, String>>> specs =
                ArgumentCaptor.forClass((Class) List.class);
        ArgumentCaptor<List<PartitionStatistics>> statistics =
                ArgumentCaptor.forClass((Class) List.class);
        ArgumentCaptor<Boolean> replaceStatistics = ArgumentCaptor.forClass(Boolean.class);
        ArgumentCaptor<List<Map<String, String>>> partitionOptions =
                ArgumentCaptor.forClass((Class) List.class);
        verify(partitionManager)
                .createPartitions(
                        specs.capture(),
                        eq(true),
                        statistics.capture(),
                        replaceStatistics.capture(),
                        partitionOptions.capture());
        List<Map<String, String>> capturedSpecs = specs.getValue();
        List<Map<String, String>> capturedOptions = partitionOptions.getValue();
        if (replaceStatistics.getValue()) {
            assertThat(capturedOptions)
                    .as("replacement options must align with every target spec")
                    .hasSameSizeAs(capturedSpecs);
            for (int i = 0; i < capturedSpecs.size(); i++) {
                assertThat(capturedOptions.get(i))
                        .as("a replacement names the partition's own default directory")
                        .isEqualTo(
                                returnsToDefault(tablePath, capturedSpecs.get(i), onlyValueInPath));
            }
        } else {
            assertThat(capturedOptions).as("append must not change partition options").isNull();
        }
        return new Reported(
                new ArrayList<>(capturedSpecs),
                new ArrayList<>(statistics.getValue()),
                replaceStatistics.getValue(),
                capturedOptions == null ? null : new ArrayList<>(capturedOptions));
    }

    /** A file IO whose deletions all report failure, leaving the files in place. */
    private static class RefusingFileIO extends LocalFileIO {

        private static final long serialVersionUID = 1L;

        @Override
        public boolean delete(Path path, boolean recursive) {
            return false;
        }
    }

    /** A file IO that refuses to delete anything under one directory. */
    private static class UndeletableFileIO extends LocalFileIO {

        private static final long serialVersionUID = 1L;

        private final String directory;

        private UndeletableFileIO(Path directory) {
            this.directory = directory.toString();
        }

        @Override
        public boolean delete(Path path, boolean recursive) throws IOException {
            if (path.toString().startsWith(directory)) {
                throw new IOException("Refused to delete " + path);
            }
            return super.delete(path, recursive);
        }
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
                boolean replaceStatistics,
                @Nullable List<Map<String, String>> partitionOptions) {
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
            List<Partition> found = new ArrayList<>();
            for (Map<String, String> partitionSpec : registered) {
                if (prefix.entrySet().stream()
                        .allMatch(
                                entry ->
                                        entry.getValue()
                                                .equals(partitionSpec.get(entry.getKey())))) {
                    found.add(partition(partitionSpec));
                }
            }
            return found;
        }

        @Override
        public List<Partition> listPartitionsByNames(List<Map<String, String>> partitions) {
            List<Partition> found = new ArrayList<>();
            for (Map<String, String> partitionSpec : partitions) {
                if (registered.contains(partitionSpec)) {
                    found.add(partition(partitionSpec));
                }
            }
            return found;
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

        Reported reported = captureAndValidateReport(partitionManager, tablePath);
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
    void testAppendRegistrationLoaderFailureDeletesPublishedTarget() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        RuntimeException failure = new RuntimeException("catalog loader failed");
        AtomicInteger loads = new AtomicInteger();
        CatalogLoader loader =
                () -> {
                    loads.incrementAndGet();
                    throw failure;
                };
        FormatTablePartitionManager partitionManager =
                FormatTablePartitionManager.create(TABLE, PARTITION_KEYS, loader);
        CommitMessage message = writtenFile(fileIO, tablePath, "year=2025/month=10", 3, 128);
        Path written = ((TwoPhaseCommitMessage) message).getCommitter().targetPath();

        assertThatThrownBy(
                        () ->
                                commit(tablePath, fileIO, partitionManager, false, null)
                                        .commit(Collections.singletonList(message)))
                .hasRootCause(failure);

        // No catalog request ran, so abort can safely delete this attempt's published file.
        assertThat(loads).hasValue(1);
        assertThat(fileIO.exists(written)).isFalse();
    }

    @Test
    void testEmptyStaticAppendOnlyRegistersPartition() {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        Map<String, String> staticPartition = spec("2025", "10");

        commit(tablePath, fileIO, partitionManager, false, staticPartition)
                .commit(Collections.emptyList());

        verify(partitionManager).createPartitions(Collections.singletonList(staticPartition), true);
        verify(partitionManager, never())
                .createPartitions(anyList(), eq(true), any(), anyBoolean(), any());
    }

    @Test
    void testAppendRegistrationBatchFailureDeletesAllTargetsWithoutReportingStatistics()
            throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        Catalog catalog = mock(Catalog.class);
        Catalog.TableNoPermissionException failure = new Catalog.TableNoPermissionException(TABLE);
        AtomicInteger requests = new AtomicInteger();
        List<Map<String, String>> registered = new ArrayList<>();
        List<PartitionStatistics> appliedStatistics = new ArrayList<>();
        when(catalog.listPartitionsPaged(TABLE, 1000, null, null))
                .thenReturn(new PagedList<>(Collections.emptyList(), null));
        when(catalog.listPartitionsByNames(eq(TABLE), anyList()))
                .thenReturn(Collections.emptyList());
        doAnswer(
                        invocation -> {
                            @SuppressWarnings("unchecked")
                            List<Map<String, String>> batch = invocation.getArgument(1);
                            @SuppressWarnings("unchecked")
                            List<PartitionStatistics> statistics = invocation.getArgument(3);
                            if (requests.incrementAndGet() == 2) {
                                // Permission is checked before the catalog mutation, so the second
                                // batch was not applied.
                                throw failure;
                            }
                            registered.addAll(batch);
                            if (statistics != null) {
                                appliedStatistics.addAll(statistics);
                            }
                            return null;
                        })
                .when(catalog)
                .createPartitions(any(), anyList(), anyBoolean(), any(), anyBoolean(), isNull());
        FormatTablePartitionManager partitionManager =
                FormatTablePartitionManager.create(TABLE, PARTITION_KEYS, () -> catalog);
        List<CommitMessage> messages = new ArrayList<>();
        List<Path> targets = new ArrayList<>();
        for (int i = 0; i < 1001; i++) {
            CommitMessage message =
                    writtenFile(fileIO, tablePath, String.format("year=2025/month=%04d", i), 1, 1);
            messages.add(message);
            targets.add(((TwoPhaseCommitMessage) message).getCommitter().targetPath());
        }

        assertThatThrownBy(
                        () ->
                                commit(tablePath, fileIO, partitionManager, false, null)
                                        .commit(messages))
                .hasRootCause(failure);

        assertThat(requests).hasValue(2);
        assertThat(registered).hasSize(1000);
        // Partition rows left by a successful registration batch are harmlessly empty. Applying
        // additive statistics before every registration succeeds would instead leave them
        // describing files this failed attempt rolls back.
        assertThat(appliedStatistics).isEmpty();
        assertThat(targets).allSatisfy(target -> assertThat(fileIO.exists(target)).isFalse());
    }

    @Test
    void testAppendStatisticsResponseLossIsNotRetriedAndDoesNotFailCommit() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        ApplyingThenFailingStatisticsManager partitionManager =
                new ApplyingThenFailingStatisticsManager();
        CommitMessage message = writtenFile(fileIO, tablePath, "year=2025/month=10", 3, 128);
        Path written = ((TwoPhaseCommitMessage) message).getCommitter().targetPath();

        // The catalog may have applied an additive report before its response was lost. Retrying
        // that report would double count it, so the data commit succeeds after one best-effort try.
        commit(tablePath, fileIO, partitionManager, false, null)
                .commit(Collections.singletonList(message));

        assertThat(partitionManager.calls).containsExactly("registration", "statistics");
        assertThat(partitionManager.statisticsAttempts).isOne();
        assertThat(partitionManager.appliedRecordCount).isEqualTo(3);
        assertThat(fileIO.exists(written)).isTrue();

        TwoPhaseCommitMessage roundTripped =
                InstantiationUtil.clone((TwoPhaseCommitMessage) message);
        commit(tablePath, fileIO, partitionManager, false, null)
                .abort(Collections.singletonList(roundTripped));
        assertThat(fileIO.exists(written)).isTrue();
    }

    @Test
    void testFailedOverwriteReportPreservesReplacementAfterDeletingOldData() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        doThrow(new RuntimeException("catalog says 429"))
                .when(partitionManager)
                .createPartitions(anyList(), anyBoolean(), any(), anyBoolean(), any());
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

        // The old file cannot be restored. Keep the replacement because the failed catalog call
        // may already have made its metadata durable.
        assertThat(fileIO.exists(written)).isTrue();
        assertThat(fileIO.exists(new Path(tablePath, "year=2025/month=10/old-data.csv"))).isFalse();
    }

    /** A catalog whose additive report takes effect before its response is lost. */
    private static class ApplyingThenFailingStatisticsManager
            implements FormatTablePartitionManager {

        private static final long serialVersionUID = 1L;

        private final List<String> calls = new ArrayList<>();
        private int statisticsAttempts;
        private long appliedRecordCount;

        @Override
        public void createPartitions(
                List<Map<String, String>> partitions,
                boolean ignoreIfExists,
                @Nullable List<PartitionStatistics> statistics,
                boolean replaceStatistics,
                @Nullable List<Map<String, String>> partitionOptions) {
            if (statistics == null) {
                calls.add("registration");
                return;
            }

            calls.add("statistics");
            statisticsAttempts++;
            for (PartitionStatistics statistic : statistics) {
                appliedRecordCount += statistic.recordCount();
            }
            throw new RuntimeException("statistics response lost");
        }

        @Override
        public List<Partition> listPartitions(
                Map<String, String> prefix, @Nullable Predicate filter) {
            return Collections.emptyList();
        }

        @Override
        public List<Partition> listPartitionsByNames(List<Map<String, String>> partitions) {
            return Collections.emptyList();
        }

        @Override
        public void dropPartitions(List<Map<String, String>> partitions) {
            throw new UnsupportedOperationException();
        }
    }

    /** What one call reported to the catalog. */
    private static class Reported {
        private final List<Map<String, String>> specs;
        private final List<PartitionStatistics> statistics;
        private final boolean replaceStatistics;
        @Nullable private final List<Map<String, String>> partitionOptions;

        private Reported(
                List<Map<String, String>> specs,
                List<PartitionStatistics> statistics,
                boolean replaceStatistics,
                @Nullable List<Map<String, String>> partitionOptions) {
            this.specs = specs;
            this.statistics = statistics;
            this.replaceStatistics = replaceStatistics;
            this.partitionOptions = partitionOptions;
        }
    }
}
