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
import org.apache.paimon.fs.RenamingTwoPhaseOutputStream;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.utils.PartitionPathUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/** Tests for {@link FormatTableCommit}. */
class FormatTableCommitTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testPartitionRegistrationFailurePreservesCommittedFiles() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        Path targetPath = new Path(tablePath, "year=2025/month=10/data-1.csv");
        RenamingTwoPhaseOutputStream outputStream =
                new RenamingTwoPhaseOutputStream(fileIO, targetPath, false);
        outputStream.write(1);
        TwoPhaseOutputStream.Committer committer = outputStream.closeForCommit();
        FormatTableCatalogProvider catalogProvider = mock(FormatTableCatalogProvider.class);
        RuntimeException registrationFailure =
                new RuntimeException("Catalog partition registration unavailable");
        doThrow(registrationFailure).when(catalogProvider).createPartitions(anyList());

        FormatTableCommit commit =
                new FormatTableCommit(
                        tablePath.toString(),
                        Arrays.asList("year", "month"),
                        fileIO,
                        false,
                        false,
                        Identifier.create("managed_db", "managed_table"),
                        null,
                        null,
                        null,
                        catalogProvider);
        CommitMessage message = new TwoPhaseCommitMessage(committer);

        assertThatThrownBy(() -> commit.commit(Collections.singletonList(message)))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Committed data files were preserved")
                .hasMessageContaining("managed_db.managed_table")
                .hasMessageContaining("MSCK REPAIR TABLE")
                .hasRootCauseMessage("Catalog partition registration unavailable");

        assertThat(fileIO.exists(targetPath)).isTrue();
        verify(catalogProvider).createPartitions(anyList());

        // Flink calls abort on the same commit object after a failed commit.
        commit.abort(Collections.singletonList(message));
        assertThat(fileIO.exists(targetPath)).isTrue();
    }

    @Test
    void testFileCommitFailureStillDiscardsUncommittedFiles() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        TwoPhaseOutputStream.Committer committer = mock(TwoPhaseOutputStream.Committer.class);
        doThrow(new IOException("data commit failed")).when(committer).commit(fileIO);
        FormatTableCatalogProvider catalogProvider = mock(FormatTableCatalogProvider.class);
        FormatTableCommit commit =
                new FormatTableCommit(
                        tablePath.toString(),
                        Arrays.asList("year", "month"),
                        fileIO,
                        false,
                        false,
                        Identifier.create("managed_db", "managed_table"),
                        null,
                        null,
                        null,
                        catalogProvider);
        CommitMessage message = new TwoPhaseCommitMessage(committer);

        assertThatThrownBy(() -> commit.commit(Collections.singletonList(message)))
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("data commit failed");

        verify(committer).discard(fileIO);
        verify(catalogProvider, never()).createPartitions(anyList());
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

        FormatTableCatalogProvider catalogProvider =
                commitPartitionedFile(tablePath, false, partitionDir);

        // The catalog must receive RAW values; readers re-escape them when probing directories.
        assertThat(registeredSpec(catalogProvider))
                .containsExactly(entry("year", "2025"), entry("month", "a b:c"));
    }

    @Test
    void testForeignKeyValueSegmentsInLocationDoNotLeakIntoSpec() throws Exception {
        Path tablePath = new Path(new Path(tempDir.toUri()), "env=prod/warehouse/tbl");

        FormatTableCatalogProvider catalogProvider =
                commitPartitionedFile(tablePath, false, "year=2025/month=10");

        assertThat(registeredSpec(catalogProvider))
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

        FormatTableCatalogProvider catalogProvider =
                commitPartitionedFile(tablePath, true, partitionDir);

        assertThat(registeredSpec(catalogProvider))
                .containsExactly(entry("year", "2025"), entry("month", "a:b"));
    }

    @Test
    void testMismatchedPartitionKeyInPathFailsWithClearMessage() {
        Path tablePath = new Path(tempDir.toUri());

        assertThatThrownBy(() -> commitPartitionedFile(tablePath, false, "year=2025/day=10"))
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .rootCause()
                .hasMessageContaining("declares partition key 'day'")
                .hasMessageContaining("partition key 'month' was expected");
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
                        true,
                        Identifier.create("managed_db", "managed_table"),
                        staticPartition,
                        null,
                        null,
                        null);

        assertThatThrownBy(() -> commit.commit(Collections.emptyList()))
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage(
                        "Partition value '..' cannot be used as a partition path component.");
        assertThat(fileIO.exists(tablePath)).isTrue();
        assertThat(fileIO.exists(siblingPath)).isTrue();
    }

    private FormatTableCatalogProvider commitPartitionedFile(
            Path tableLocation, boolean onlyValueInPath, String partitionDir) throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path targetPath = new Path(new Path(tableLocation, partitionDir), "data-1.csv");
        RenamingTwoPhaseOutputStream outputStream =
                new RenamingTwoPhaseOutputStream(fileIO, targetPath, false);
        outputStream.write(1);
        TwoPhaseOutputStream.Committer committer = outputStream.closeForCommit();
        FormatTableCatalogProvider catalogProvider = mock(FormatTableCatalogProvider.class);
        FormatTableCommit commit =
                new FormatTableCommit(
                        tableLocation.toString(),
                        Arrays.asList("year", "month"),
                        fileIO,
                        onlyValueInPath,
                        false,
                        Identifier.create("managed_db", "managed_table"),
                        null,
                        null,
                        null,
                        catalogProvider);
        commit.commit(Collections.singletonList(new TwoPhaseCommitMessage(committer)));
        return catalogProvider;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Map<String, String> registeredSpec(FormatTableCatalogProvider catalogProvider) {
        ArgumentCaptor<List<Map<String, String>>> captor =
                ArgumentCaptor.forClass((Class) List.class);
        verify(catalogProvider).createPartitions(captor.capture());
        assertThat(captor.getValue()).hasSize(1);
        return captor.getValue().get(0);
    }
}
