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
import org.apache.paimon.table.sink.CommitMessage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.paimon.CoreOptions.PARTITION_DEFAULT_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for the partition mark done support of {@link FormatTableCommit}. */
class FormatTableCommitMarkDoneTest {

    @TempDir java.nio.file.Path tempDir;

    private final LocalFileIO fileIO = LocalFileIO.create();
    private Path tablePath;

    @BeforeEach
    void setUp() throws IOException {
        tablePath = new Path(tempDir.toUri());
        for (String partition : Arrays.asList("dt=1/hh=08", "dt=1/hh=09", "dt=2/hh=08")) {
            fileIO.overwriteFileUtf8(new Path(tablePath, partition + "/data.csv"), "1");
            fileIO.overwriteFileUtf8(new Path(tablePath, partition + "/_SUCCESS"), "{}");
        }
        // Marked done while empty.
        fileIO.overwriteFileUtf8(new Path(tablePath, "dt=1/hh=10/_SUCCESS"), "{}");
    }

    @Test
    void testCommittedPartitions() {
        FormatTableCommit append = commit(fileIO, false, null, false);
        append.commit(Arrays.asList(message("dt=3/hh=08/a"), message("dt=3/hh=09/b")));
        assertThat(append.committedPartitions()).containsExactly(spec("3", "08"), spec("3", "09"));
        assertThat(new ArrayList<>(append.committedPartitions().get(0).keySet()))
                .containsExactly("dt", "hh");

        // An overwrite of a full static partition reports it even without rows.
        Map<String, String> staticPartition = new HashMap<>();
        staticPartition.put("hh", "00");
        staticPartition.put("dt", "4");
        FormatTableCommit emptyOverwrite = commit(fileIO, true, staticPartition, false);
        emptyOverwrite.commit(Collections.emptyList());
        assertThat(emptyOverwrite.committedPartitions()).containsExactly(spec("4", "00"));

        FormatTableCommit prefixOverwrite =
                commit(fileIO, true, Collections.singletonMap("dt", "3"), false);
        prefixOverwrite.commit(Collections.singletonList(message("dt=3/hh=09/c")));
        assertThat(prefixOverwrite.committedPartitions()).containsExactly(spec("3", "09"));
    }

    @Test
    void testFailedCommitReportsNothing() throws IOException {
        FormatTableCommit commit = commit(fileIO, false, null, false);
        commit.commit(Collections.singletonList(message("dt=3/hh=08/a")));

        TwoPhaseOutputStream.Committer failing = mock(TwoPhaseOutputStream.Committer.class);
        when(failing.targetPath()).thenReturn(new Path(tablePath, "dt=3/hh=09/b"));
        doThrow(new IOException("publish failed")).when(failing).commit(any());
        assertThatThrownBy(
                () -> commit.commit(Collections.singletonList(new TwoPhaseCommitMessage(failing))));
        assertThat(commit.committedPartitions()).isEmpty();
    }

    @Test
    void testOverwriteDeletesSuccessFilesOfClearedPartitions() {
        commit(fileIO, true, Collections.singletonMap("dt", "1"), true)
                .commit(Collections.singletonList(message("dt=1/hh=09/new")));
        assertThat(successFile("dt=1/hh=08")).isFalse();
        assertThat(successFile("dt=1/hh=09")).isFalse();
        assertThat(successFile("dt=1/hh=10")).isFalse();
        assertThat(successFile("dt=2/hh=08")).isTrue();

        commit(fileIO, true, null, true).commit(Collections.emptyList());
        assertThat(successFile("dt=2/hh=08")).isFalse();
    }

    @Test
    void testValueOnlyOverwriteDeletesSuccessFilesOfClearedPartitions() throws IOException {
        Path valueOnly = new Path(tablePath, "value_only");
        for (String partition : Arrays.asList("1/08", "1/09", "2/08")) {
            fileIO.overwriteFileUtf8(new Path(valueOnly, partition + "/data.csv"), "1");
            fileIO.overwriteFileUtf8(new Path(valueOnly, partition + "/_SUCCESS"), "{}");
        }
        fileIO.overwriteFileUtf8(new Path(valueOnly, "1/10/_SUCCESS"), "{}");

        commit(valueOnly, true, fileIO, true, Collections.singletonMap("dt", "1"), true, false)
                .commit(Collections.singletonList(message("value_only/1/09/new")));
        assertThat(successFile("value_only/1/08")).isFalse();
        assertThat(successFile("value_only/1/09")).isFalse();
        assertThat(successFile("value_only/1/10")).isFalse();
        assertThat(successFile("value_only/2/08")).isTrue();
    }

    @Test
    void testDynamicOverwriteDeletesSuccessFilesOfWrittenPartitions() {
        commit(fileIO, true, null, true, true)
                .commit(Collections.singletonList(message("dt=1/hh=08/new")));

        assertThat(successFile("dt=1/hh=08")).isFalse();
        assertThat(successFile("dt=1/hh=09")).isTrue();
    }

    @Test
    void testAppendAndTruncateDeleteSuccessFiles() {
        commit(fileIO, false, null, true)
                .commit(Collections.singletonList(message("dt=1/hh=08/new")));
        assertThat(successFile("dt=1/hh=08")).isFalse();
        assertThat(successFile("dt=1/hh=09")).isTrue();

        commit(fileIO, false, null, true)
                .truncatePartitions(Collections.singletonList(Collections.singletonMap("dt", "1")));
        assertThat(successFile("dt=1/hh=09")).isFalse();
        assertThat(successFile("dt=2/hh=08")).isTrue();

        commit(fileIO, false, null, true).truncateTable();
        assertThat(successFile("dt=2/hh=08")).isFalse();
    }

    @Test
    void testSuccessFilesStayWhenDeletionIsOff() {
        commit(fileIO, true, null, false).commit(Collections.emptyList());

        assertThat(successFile("dt=1/hh=08")).isTrue();
        assertThat(exists("dt=1/hh=08/data.csv")).isFalse();
    }

    @Test
    void testRefusedDeletionFailsBeforeDataIsDeleted() {
        FileIO refusing =
                new LocalFileIO() {
                    @Override
                    public boolean delete(Path path, boolean recursive) throws IOException {
                        return !path.getName().equals("_SUCCESS") && super.delete(path, recursive);
                    }
                };

        assertThatThrownBy(() -> commit(refusing, true, null, true).commit(Collections.emptyList()))
                .hasStackTraceContaining("Failed to delete success file");
        assertThat(exists("dt=1/hh=08/data.csv")).isTrue();
    }

    private FormatTableCommit commit(
            FileIO fileIO,
            boolean overwrite,
            @Nullable Map<String, String> staticPartition,
            boolean deletesSuccessFiles) {
        return commit(fileIO, overwrite, staticPartition, deletesSuccessFiles, false);
    }

    private FormatTableCommit commit(
            FileIO fileIO,
            boolean overwrite,
            @Nullable Map<String, String> staticPartition,
            boolean deletesSuccessFiles,
            boolean dynamicPartitionOverwrite) {
        return commit(
                tablePath,
                false,
                fileIO,
                overwrite,
                staticPartition,
                deletesSuccessFiles,
                dynamicPartitionOverwrite);
    }

    private FormatTableCommit commit(
            Path location,
            boolean onlyValueInPath,
            FileIO fileIO,
            boolean overwrite,
            @Nullable Map<String, String> staticPartition,
            boolean deletesSuccessFiles,
            boolean dynamicPartitionOverwrite) {
        return new FormatTableCommit(
                        location.toString(),
                        Arrays.asList("dt", "hh"),
                        fileIO,
                        onlyValueInPath,
                        PARTITION_DEFAULT_NAME.defaultValue(),
                        overwrite,
                        Identifier.create("db", "events"),
                        staticPartition,
                        null,
                        null,
                        null,
                        dynamicPartitionOverwrite)
                .withSuccessFileDeletion(deletesSuccessFiles);
    }

    private CommitMessage message(String file) {
        TwoPhaseOutputStream.Committer committer = mock(TwoPhaseOutputStream.Committer.class);
        when(committer.targetPath()).thenReturn(new Path(tablePath, file));
        return new TwoPhaseCommitMessage(committer);
    }

    private boolean successFile(String partition) {
        return exists(partition + "/_SUCCESS");
    }

    private boolean exists(String relative) {
        try {
            return fileIO.exists(new Path(tablePath, relative));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<String, String> spec(String dt, String hh) {
        Map<String, String> spec = new LinkedHashMap<>();
        spec.put("dt", dt);
        spec.put("hh", hh);
        return spec;
    }
}
