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

import static org.apache.paimon.CoreOptions.PARTITION_DEFAULT_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/** Tests for {@link FormatTableCommit}. */
class FormatTableCommitTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testPartitionRegistrationFailureDiscardsTheFilesItWrote() throws Exception {
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
                        partitionManager);
        CommitMessage message = new TwoPhaseCommitMessage(committer);

        assertThatThrownBy(() -> commit.commit(Collections.singletonList(message)))
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("Catalog partition registration unavailable");

        // A failed write leaves nothing behind, whichever step failed: rerunning it converges,
        // and an idempotent registration makes a partition that was registered anyway harmless.
        assertThat(fileIO.exists(targetPath)).isFalse();
        verify(partitionManager).createPartitions(anyList(), eq(true));
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
                        partitionManager);
        CommitMessage message = new TwoPhaseCommitMessage(committer);

        assertThatThrownBy(() -> commit.commit(Collections.singletonList(message)))
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("data commit failed");

        verify(committer).discard(fileIO);
        verify(partitionManager, never()).createPartitions(anyList(), eq(true));
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
        // ('_temporary' is the other such tree, but this writer's own clean() still empties it -
        //  covered once that is fixed separately.)
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
                        null);

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
                        null);

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
                        null);

        commit.commit(Collections.emptyList());

        assertThat(fileIO.exists(staleFile)).isFalse();
        assertThat(fileIO.exists(staleSibling)).isFalse();
        // Inside the partition, hidden still means staging.
        assertThat(fileIO.exists(stagedFile)).isTrue();
        assertThat(fileIO.exists(stagedNextToThePartitions)).isTrue();
    }

    @Test
    void testPathNotMatchingThePartitionKeysFails() {
        Path tablePath = new Path(tempDir.toUri());

        // The message names the path and the declared keys, which is what tells a reader that
        // 'day' is not where 'month' was expected.
        assertThatThrownBy(() -> commitPartitionedFile(tablePath, false, "year=2025/day=10"))
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .rootCause()
                .hasMessageContaining("year=2025/day=10")
                .hasMessageContaining("catalog_partition_db.catalog_partition_table")
                .hasMessageContaining("[year, month]");
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
                        null);

        assertThatThrownBy(() -> commit.commit(Collections.emptyList()))
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage(
                        "Partition value '..' cannot be used as a partition path component.");
        assertThat(fileIO.exists(tablePath)).isTrue();
        assertThat(fileIO.exists(siblingPath)).isTrue();
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
                        partitionManager);
        commit.commit(Collections.singletonList(new TwoPhaseCommitMessage(committer)));
        return partitionManager;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Map<String, String> registeredSpec(
            FormatTablePartitionManager partitionManager) {
        ArgumentCaptor<List<Map<String, String>>> captor =
                ArgumentCaptor.forClass((Class) List.class);
        verify(partitionManager).createPartitions(captor.capture(), eq(true));
        assertThat(captor.getValue()).hasSize(1);
        return captor.getValue().get(0);
    }
}
