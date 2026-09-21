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
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionStatistics;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.CoreOptions.PARTITION_DEFAULT_NAME;
import static org.apache.paimon.CoreOptions.PATH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Partition registry validation tests for {@link FormatTableCommit}. */
class FormatTableCommitRegistryValidationTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testAppendValidatesOnlyAffectedRowsReturnedByName() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(new Path(tempDir.toUri()), "append-scoped-registry");
        Map<String, String> targetSpec = partitionSpec("2025", "10");
        Map<String, String> unrelatedMalformedSpec = Collections.singletonMap("year", "2024");
        TwoPhaseOutputStream.Committer committer = mock(TwoPhaseOutputStream.Committer.class);
        when(committer.targetPath())
                .thenReturn(new Path(tablePath, "year=2025/month=10/data-new.csv"));
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        when(partitionManager.listPartitionsByNames(Collections.singletonList(targetSpec)))
                .thenReturn(Collections.singletonList(partitionAt(targetSpec, null)));
        when(partitionManager.listPartitions(Collections.emptyMap(), null))
                .thenReturn(Collections.singletonList(partitionAt(unrelatedMalformedSpec, null)));
        FormatTableCommit commit = commit(tablePath, fileIO, partitionManager, false, null, true);

        commit.commit(Collections.singletonList(new TwoPhaseCommitMessage(committer)));

        verify(partitionManager).listPartitionsByNames(Collections.singletonList(targetSpec));
        verify(partitionManager, never()).listPartitions(anyMap(), isNull());
        verify(committer).commit(fileIO);
    }

    @Test
    void testAppendRejectsMalformedAffectedRowBeforePublishing() throws Exception {
        MutationTrackingLocalFileIO fileIO = new MutationTrackingLocalFileIO();
        Path tablePath = new Path(new Path(tempDir.toUri()), "append-malformed-affected-row");
        Map<String, String> targetSpec = partitionSpec("2025", "10");
        Map<String, String> incompleteSpec = Collections.singletonMap("year", "2025");
        TwoPhaseOutputStream.Committer committer = mock(TwoPhaseOutputStream.Committer.class);
        when(committer.targetPath())
                .thenReturn(new Path(tablePath, "year=2025/month=10/data-new.csv"));
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        when(partitionManager.listPartitionsByNames(Collections.singletonList(targetSpec)))
                .thenReturn(Collections.singletonList(partitionAt(incompleteSpec, null)));
        FormatTableCommit commit = commit(tablePath, fileIO, partitionManager, false, null, true);
        fileIO.startTrackingMutations();

        assertThatThrownBy(
                        () ->
                                commit.commit(
                                        Collections.singletonList(
                                                new TwoPhaseCommitMessage(committer))))
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseMessage(
                        "Catalog returned incomplete partition spec {year=2025} for Format Table "
                                + "location_db.location_table.");

        assertThat(fileIO.deleteCalls()).isZero();
        assertThat(fileIO.mkdirsCalls()).isZero();
        verify(committer, never()).commit(fileIO);
        verify(committer, never()).clean(fileIO);
        verify(partitionManager, never()).listPartitions(anyMap(), isNull());
        verify(partitionManager, never())
                .createPartitions(anyList(), anyBoolean(), any(), anyBoolean(), any());
    }

    @Test
    void testStaticPrefixOverwriteValidatesOnlyRowsReturnedForPrefix() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(new Path(tempDir.toUri()), "overwrite-scoped-prefix");
        Map<String, String> prefix = Collections.singletonMap("year", "2025");
        Map<String, String> targetSpec = partitionSpec("2025", "10");
        Map<String, String> unrelatedMalformedSpec = Collections.singletonMap("year", "2024");
        Path targetData = new Path(tablePath, "year=2025/month=10/data-old.csv");
        Path unrelatedData = new Path(tablePath, "year=2024/month=11/data-old.csv");
        fileIO.writeFile(targetData, "target", false);
        fileIO.writeFile(unrelatedData, "unrelated", false);
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        when(partitionManager.listPartitions(prefix, null))
                .thenReturn(Collections.singletonList(partitionAt(targetSpec, null)));
        when(partitionManager.listPartitions(Collections.emptyMap(), null))
                .thenReturn(Collections.singletonList(partitionAt(unrelatedMalformedSpec, null)));
        FormatTableCommit commit = commit(tablePath, fileIO, partitionManager, true, prefix, true);

        commit.commit(Collections.emptyList());

        assertThat(fileIO.exists(targetData)).isFalse();
        assertThat(fileIO.exists(unrelatedData)).isTrue();
        verify(partitionManager).listPartitions(prefix, null);
        verify(partitionManager, never()).listPartitions(Collections.emptyMap(), null);
        verify(partitionManager, never()).listPartitionsByNames(anyList());
    }

    @Test
    void testTruncatePrefixValidatesOnlyRowsReturnedForPrefix() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(new Path(tempDir.toUri()), "truncate-scoped-prefix");
        Map<String, String> prefix = Collections.singletonMap("year", "2025");
        Map<String, String> targetSpec = partitionSpec("2025", "10");
        Map<String, String> unrelatedMalformedSpec = Collections.singletonMap("year", "2024");
        Path targetData = new Path(tablePath, "year=2025/month=10/data-old.csv");
        Path unrelatedData = new Path(tablePath, "year=2024/month=11/data-old.csv");
        fileIO.writeFile(targetData, "target", false);
        fileIO.writeFile(unrelatedData, "unrelated", false);
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        when(partitionManager.listPartitions(prefix, null))
                .thenReturn(Collections.singletonList(partitionAt(targetSpec, null)));
        when(partitionManager.listPartitions(Collections.emptyMap(), null))
                .thenReturn(Collections.singletonList(partitionAt(unrelatedMalformedSpec, null)));
        FormatTableCommit commit = commit(tablePath, fileIO, partitionManager, false, null, true);

        commit.truncatePartitions(Collections.singletonList(prefix));

        assertThat(fileIO.exists(targetData)).isFalse();
        assertThat(fileIO.exists(unrelatedData)).isTrue();
        verify(partitionManager).listPartitions(prefix, null);
        verify(partitionManager, never()).listPartitions(Collections.emptyMap(), null);
        verify(partitionManager, never()).listPartitionsByNames(anyList());
    }

    @Test
    void testTruncateMixedExactAndPrefixTargetsUsesScopedLookupsAndDeduplicates() throws Exception {
        MutationTrackingLocalFileIO fileIO = new MutationTrackingLocalFileIO();
        Path tablePath = new Path(new Path(tempDir.toUri()), "truncate-mixed-targets");
        Map<String, String> exactOutsidePrefix = partitionSpec("2024", "12");
        Map<String, String> exactInsidePrefix = partitionSpec("2025", "10");
        Map<String, String> prefixOnly = partitionSpec("2025", "11");
        Map<String, String> prefix = Collections.singletonMap("year", "2025");
        Path exactOutsideData = new Path(tablePath, "year=2024/month=12/data-exact-outside.csv");
        Path exactInsideData = new Path(tablePath, "year=2025/month=10/data-exact-inside.csv");
        Path prefixOnlyData = new Path(tablePath, "year=2025/month=11/data-prefix-only.csv");
        Path unrelatedData = new Path(tablePath, "year=2023/month=01/data-unrelated.csv");
        fileIO.writeFile(exactOutsideData, "outside", false);
        fileIO.writeFile(exactInsideData, "inside", false);
        fileIO.writeFile(prefixOnlyData, "prefix", false);
        fileIO.writeFile(unrelatedData, "unrelated", false);
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        List<Map<String, String>> exactSpecs = Arrays.asList(exactOutsidePrefix, exactInsidePrefix);
        Partition exactInsidePartition = partitionAt(exactInsidePrefix, null);
        when(partitionManager.listPartitionsByNames(exactSpecs))
                .thenReturn(
                        Arrays.asList(partitionAt(exactOutsidePrefix, null), exactInsidePartition));
        when(partitionManager.listPartitions(prefix, null))
                .thenReturn(Arrays.asList(exactInsidePartition, partitionAt(prefixOnly, null)));
        FormatTableCommit commit = commit(tablePath, fileIO, partitionManager, false, null, true);
        fileIO.startTrackingMutations();

        commit.truncatePartitions(Arrays.asList(exactOutsidePrefix, exactInsidePrefix, prefix));

        assertThat(fileIO.exists(exactOutsideData)).isFalse();
        assertThat(fileIO.exists(exactInsideData)).isFalse();
        assertThat(fileIO.exists(prefixOnlyData)).isFalse();
        assertThat(fileIO.exists(unrelatedData)).isTrue();
        assertThat(fileIO.deleteCalls()).isEqualTo(3);
        assertThat(fileIO.listCalls(exactInsideData.getParent())).isEqualTo(1);
        assertThat(fileIO.mkdirsCalls()).isZero();
        verify(partitionManager).listPartitionsByNames(exactSpecs);
        verify(partitionManager).listPartitions(prefix, null);
        verify(partitionManager, never()).listPartitions(Collections.emptyMap(), null);
        assertReplacementReport(
                partitionManager,
                tablePath,
                Arrays.asList(exactOutsidePrefix, exactInsidePrefix, prefixOnly));
    }

    @Test
    void testWholeTableTruncateValidatesFullRegistryBeforeMutation() throws Exception {
        MutationTrackingLocalFileIO fileIO = new MutationTrackingLocalFileIO();
        Path tablePath = new Path(new Path(tempDir.toUri()), "truncate-full-registry");
        Map<String, String> validSpec = partitionSpec("2025", "10");
        Map<String, String> incompleteSpec = Collections.singletonMap("year", "2024");
        Path validData = new Path(tablePath, "year=2025/month=10/data-old.csv");
        fileIO.writeFile(validData, "old", false);
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        when(partitionManager.listPartitions(Collections.emptyMap(), null))
                .thenReturn(
                        Arrays.asList(
                                partitionAt(validSpec, null), partitionAt(incompleteSpec, null)));
        FormatTableCommit commit = commit(tablePath, fileIO, partitionManager, false, null, true);
        fileIO.startTrackingMutations();

        assertThatThrownBy(commit::truncateTable)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Catalog returned incomplete partition spec {year=2024} for Format Table "
                                + "location_db.location_table.");

        assertThat(fileIO.exists(validData)).isTrue();
        assertThat(fileIO.deleteCalls()).isZero();
        assertThat(fileIO.mkdirsCalls()).isZero();
        verify(partitionManager).listPartitions(Collections.emptyMap(), null);
        verify(partitionManager, never()).listPartitionsByNames(anyList());
        verify(partitionManager, never())
                .createPartitions(anyList(), anyBoolean(), any(), anyBoolean(), any());
    }

    @Test
    void testTruncateRejectsNonLeadingPrefixBeforeMutationWhenFullRegistryIsRequired()
            throws Exception {
        MutationTrackingLocalFileIO fileIO = new MutationTrackingLocalFileIO();
        Path tablePath = new Path(new Path(tempDir.toUri()), "truncate-non-leading-prefix");
        Map<String, String> november2024 = partitionSpec("2024", "11");
        Map<String, String> november2025 = partitionSpec("2025", "11");
        Path data2024 = new Path(tablePath, "year=2024/month=11/data-2024.csv");
        Path data2025 = new Path(tablePath, "year=2025/month=11/data-2025.csv");
        fileIO.writeFile(data2024, "2024", false);
        fileIO.writeFile(data2025, "2025", false);
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        when(partitionManager.listPartitions(Collections.emptyMap(), null))
                .thenReturn(
                        Arrays.asList(
                                partitionAt(november2024, null), partitionAt(november2025, null)));
        FormatTableCommit commit =
                new FormatTableCommit(
                        tablePath.toString(),
                        Arrays.asList("year", "month"),
                        fileIO,
                        false,
                        PARTITION_DEFAULT_NAME.defaultValue(),
                        false,
                        Identifier.create("location_db", "location_table"),
                        null,
                        null,
                        null,
                        partitionManager,
                        /* dynamicPartitionOverwrite */ true);
        fileIO.startTrackingMutations();

        assertThatThrownBy(
                        () ->
                                commit.truncatePartitions(
                                        Collections.singletonList(
                                                Collections.singletonMap("month", "11"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Partition spec {month=11} is not a leading prefix of partition keys "
                                + "[year, month].");

        assertThat(fileIO.exists(data2024)).isTrue();
        assertThat(fileIO.exists(data2025)).isTrue();
        assertThat(fileIO.deleteCalls()).isZero();
        assertThat(fileIO.mkdirsCalls()).isZero();
        verify(partitionManager, never()).listPartitions(anyMap(), isNull());
        verify(partitionManager, never())
                .createPartitions(anyList(), anyBoolean(), any(), anyBoolean(), any());
    }

    private static Partition partitionAt(Map<String, String> spec, String location) {
        Map<String, String> options =
                location == null ? null : Collections.singletonMap(PATH.key(), location);
        return new Partition(
                spec,
                0,
                0,
                0,
                0,
                PartitionStatistics.UNKNOWN_TOTAL_BUCKETS,
                false,
                null,
                null,
                null,
                null,
                options);
    }

    private static FormatTableCommit commit(
            Path tablePath,
            LocalFileIO fileIO,
            FormatTablePartitionManager partitionManager,
            boolean overwrite,
            Map<String, String> staticPartitions,
            boolean dynamicPartitionOverwrite) {
        return new FormatTableCommit(
                tablePath.toString(),
                Arrays.asList("year", "month"),
                fileIO,
                false,
                PARTITION_DEFAULT_NAME.defaultValue(),
                overwrite,
                Identifier.create("location_db", "location_table"),
                staticPartitions,
                null,
                null,
                partitionManager,
                dynamicPartitionOverwrite);
    }

    private static Map<String, String> partitionSpec(String year, String month) {
        LinkedHashMap<String, String> spec = new LinkedHashMap<>();
        spec.put("year", year);
        spec.put("month", month);
        return spec;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void assertReplacementReport(
            FormatTablePartitionManager partitionManager,
            Path tablePath,
            List<Map<String, String>> expectedSpecs) {
        ArgumentCaptor<List<Map<String, String>>> specs =
                ArgumentCaptor.forClass((Class) List.class);
        ArgumentCaptor<List<PartitionStatistics>> statistics =
                ArgumentCaptor.forClass((Class) List.class);
        ArgumentCaptor<List<Map<String, String>>> options =
                ArgumentCaptor.forClass((Class) List.class);
        verify(partitionManager)
                .createPartitions(
                        specs.capture(),
                        eq(true),
                        statistics.capture(),
                        eq(true),
                        options.capture());
        assertThat(specs.getValue()).containsExactlyInAnyOrderElementsOf(expectedSpecs);
        assertThat(statistics.getValue())
                .extracting(PartitionStatistics::spec)
                .containsExactlyInAnyOrderElementsOf(expectedSpecs);
        List<Map<String, String>> reportedSpecs = specs.getValue();
        assertThat(options.getValue()).hasSameSizeAs(expectedSpecs);
        for (int i = 0; i < reportedSpecs.size(); i++) {
            Map<String, String> spec = reportedSpecs.get(i);
            assertThat(options.getValue().get(i))
                    .as("a replacement names the partition's own default directory")
                    .isEqualTo(
                            Collections.singletonMap(
                                    PATH.key(),
                                    new Path(
                                                    tablePath,
                                                    "year="
                                                            + spec.get("year")
                                                            + "/month="
                                                            + spec.get("month"))
                                            .toString()));
        }
    }

    private static class MutationTrackingLocalFileIO extends LocalFileIO {

        private final AtomicInteger deleteCalls = new AtomicInteger();
        private final AtomicInteger mkdirsCalls = new AtomicInteger();
        private final Map<Path, AtomicInteger> listCalls = new LinkedHashMap<>();
        private boolean tracking;

        private void startTrackingMutations() {
            deleteCalls.set(0);
            mkdirsCalls.set(0);
            listCalls.clear();
            tracking = true;
        }

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            if (tracking) {
                listCalls.computeIfAbsent(path, ignored -> new AtomicInteger()).incrementAndGet();
            }
            return super.listStatus(path);
        }

        @Override
        public boolean delete(Path path, boolean recursive) throws IOException {
            if (tracking) {
                deleteCalls.incrementAndGet();
            }
            return super.delete(path, recursive);
        }

        @Override
        public boolean mkdirs(Path path) throws IOException {
            if (tracking) {
                mkdirsCalls.incrementAndGet();
            }
            return super.mkdirs(path);
        }

        private int deleteCalls() {
            return deleteCalls.get();
        }

        private int mkdirsCalls() {
            return mkdirsCalls.get();
        }

        private int listCalls(Path path) {
            AtomicInteger calls = listCalls.get(path);
            return calls == null ? 0 : calls.get();
        }
    }
}
