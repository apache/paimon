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

package org.apache.paimon.table;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.RenamingTwoPhaseOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.table.format.FormatTableCommit;
import org.apache.paimon.table.format.FormatTablePartitionManager;
import org.apache.paimon.table.format.TwoPhaseCommitMessage;
import org.apache.paimon.table.sink.CommitMessage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Compatibility tests for the public Format Table commit API. */
class FormatTableCommitCompatibilityTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testLegacyPublicConstructorCanCommitOutsideFormatPackage() throws Exception {
        Path tablePath = new Path(tempDir.toUri());
        try (FormatTableCommit commit =
                new FormatTableCommit(
                        tablePath.toString(),
                        Collections.emptyList(),
                        LocalFileIO.create(),
                        false,
                        "__DEFAULT_PARTITION__",
                        false,
                        Identifier.create("compatibility_db", "compatibility_table"),
                        null,
                        null,
                        null,
                        null,
                        true)) {
            commit.commit(Collections.emptyList());
        }
    }

    @ParameterizedTest
    @CsvSource({
        "append, false",
        "append, true",
        "static, false",
        "static, true",
        "dynamic, false",
        "dynamic, true",
        "prefix, false",
        "prefix, true",
        "table, false",
        "table, true",
        "empty-static, false",
        "empty-static, true"
    })
    void testLegacyConstructorRequiresWriteFormatForExplicitPartitions(
            String operation, boolean explicitFormat) throws Exception {
        Path tablePath = new Path(tempDir.toUri());
        LocalFileIO fileIO = LocalFileIO.create();
        Path partitionPath = new Path(tablePath, "pt=p/sub=s");
        fileIO.mkdirs(partitionPath);
        Path oldFile = new Path(partitionPath, "old.parquet");
        Path newFile = new Path(partitionPath, "new.parquet");
        fileIO.overwriteFileUtf8(oldFile, "old");
        List<CommitMessage> messages;
        if (operation.equals("empty-static")) {
            messages = Collections.emptyList();
        } else {
            RenamingTwoPhaseOutputStream stream =
                    new RenamingTwoPhaseOutputStream(fileIO, newFile, false);
            stream.write("replacement".getBytes(StandardCharsets.UTF_8));
            messages =
                    Collections.singletonList(new TwoPhaseCommitMessage(stream.closeForCommit()));
        }

        Map<String, String> spec = new LinkedHashMap<>();
        spec.put("pt", "p");
        spec.put("sub", "s");
        Partition partition =
                new Partition(
                        spec,
                        1,
                        3,
                        1,
                        0,
                        -1,
                        false,
                        null,
                        null,
                        null,
                        null,
                        explicitFormat ? Collections.singletonMap("file.format", "parquet") : null);
        FormatTablePartitionManager manager = mock(FormatTablePartitionManager.class);
        when(manager.listPartitions(anyMap(), any()))
                .thenReturn(Collections.singletonList(partition));
        when(manager.listPartitionsByNames(anyList()))
                .thenReturn(Collections.singletonList(partition));
        Map<String, String> staticSpec = Collections.emptyMap();
        if (operation.equals("static") || operation.equals("empty-static")) {
            staticSpec = spec;
        } else if (operation.equals("prefix")) {
            staticSpec = Collections.singletonMap("pt", "p");
        }

        try (FormatTableCommit commit =
                new FormatTableCommit(
                        tablePath.toString(),
                        Arrays.asList("pt", "sub"),
                        fileIO,
                        false,
                        "__DEFAULT_PARTITION__",
                        !operation.equals("append"),
                        Identifier.create("compatibility_db", "compatibility_table"),
                        staticSpec,
                        null,
                        null,
                        manager,
                        operation.equals("dynamic"))) {
            if (explicitFormat) {
                assertThatThrownBy(() -> commit.commit(messages))
                        .hasRootCauseInstanceOf(UnsupportedOperationException.class)
                        .hasStackTraceContaining("write format is unknown")
                        .hasStackTraceContaining("FormatTable.newBatchWriteBuilder()");
                assertThat(fileIO.readFileUtf8(oldFile)).isEqualTo("old");
                assertThat(fileIO.exists(newFile)).isFalse();
                verify(manager, never())
                        .createPartitions(anyList(), anyBoolean(), any(), anyBoolean(), any());
            } else {
                commit.commit(messages);
                assertThat(fileIO.exists(oldFile)).isEqualTo(operation.equals("append"));
                assertThat(fileIO.exists(newFile)).isEqualTo(!messages.isEmpty());
            }
        }
        if (!messages.isEmpty()) {
            assertThat(fileIO.listStatus(new Path(partitionPath, "_temporary"))).isEmpty();
        }
    }
}
