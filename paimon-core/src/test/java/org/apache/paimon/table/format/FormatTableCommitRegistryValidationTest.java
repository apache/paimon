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
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionStatistics;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
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
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Partition registry validation tests for {@link FormatTableCommit}. */
class FormatTableCommitRegistryValidationTest {

    @TempDir java.nio.file.Path tempDir;

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

    private static Map<String, String> partitionSpec(String year, String month) {
        LinkedHashMap<String, String> spec = new LinkedHashMap<>();
        spec.put("year", year);
        spec.put("month", month);
        return spec;
    }

    private static class MutationTrackingLocalFileIO extends LocalFileIO {

        private final AtomicInteger deleteCalls = new AtomicInteger();
        private final AtomicInteger mkdirsCalls = new AtomicInteger();
        private boolean tracking;

        private void startTrackingMutations() {
            deleteCalls.set(0);
            mkdirsCalls.set(0);
            tracking = true;
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
    }
}
