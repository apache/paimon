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
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.utils.PartitionPathUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.CoreOptions.PARTITION_DEFAULT_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * A partition value is escaped into one directory name: {@code a%b} is written {@code a%25b}, and
 * {@code a/b} is one level named {@code a%2Fb} rather than two. The directory an overwrite or a
 * truncate names has to be that name, or the catalog cannot tell it is the partition's own.
 */
class FormatTableCommitEscapedPartitionValueTest {

    private static final List<String> VALUES = Arrays.asList("a%b", "a/b", "a=b");

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testOverwriteNamesTheEscapedDefaultDirectoryOfEveryPartition() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(new Path(tempDir.toUri()), "escaped-overwrite");
        List<Map<String, String>> specs = new ArrayList<>();
        List<CommitMessage> messages = new ArrayList<>();
        for (String value : VALUES) {
            Map<String, String> spec = Collections.singletonMap("part", value);
            specs.add(spec);
            Path directory = new Path(tablePath, partitionDirectory(spec));
            fileIO.writeFile(new Path(directory, "data-old.csv"), "old", false);
            TwoPhaseOutputStream.Committer committer = mock(TwoPhaseOutputStream.Committer.class);
            when(committer.targetPath()).thenReturn(new Path(directory, "data-new.csv"));
            messages.add(new TwoPhaseCommitMessage(committer));
        }
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);

        commit(tablePath, fileIO, partitionManager, /* dynamicPartitionOverwrite */ true)
                .commit(messages);

        assertReportedDirectories(partitionManager, tablePath, specs);
    }

    @Test
    void testTruncateNamesTheEscapedDefaultDirectoryAndLeavesExternalData() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(new Path(tempDir.toUri()), "escaped-truncate");
        Path externalRoot = new Path(new Path(tempDir.toUri()), "escaped-external");
        List<Map<String, String>> specs = new ArrayList<>();
        List<Partition> registered = new ArrayList<>();
        List<Path> externalData = new ArrayList<>();
        for (int i = 0; i < VALUES.size(); i++) {
            Map<String, String> spec = Collections.singletonMap("part", VALUES.get(i));
            specs.add(spec);
            fileIO.writeFile(
                    new Path(new Path(tablePath, partitionDirectory(spec)), "data-old.csv"),
                    "old",
                    false);
            Path external = new Path(new Path(externalRoot, "external-" + i), "data.csv");
            fileIO.writeFile(external, "external", false);
            externalData.add(external);
            registered.add(partitionAt(spec, new Path(externalRoot, "external-" + i).toString()));
        }
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        when(partitionManager.listPartitionsByNames(specs)).thenReturn(registered);

        commit(tablePath, fileIO, partitionManager, /* dynamicPartitionOverwrite */ false)
                .truncatePartitions(specs);

        for (Path external : externalData) {
            assertThat(fileIO.exists(external)).isTrue();
        }
        assertReportedDirectories(partitionManager, tablePath, specs);
    }

    private FormatTableCommit commit(
            Path tablePath,
            LocalFileIO fileIO,
            FormatTablePartitionManager partitionManager,
            boolean dynamicPartitionOverwrite) {
        return new FormatTableCommit(
                tablePath.toString(),
                Collections.singletonList("part"),
                fileIO,
                false,
                PARTITION_DEFAULT_NAME.defaultValue(),
                dynamicPartitionOverwrite,
                Identifier.create("escaped_db", "escaped_table"),
                null,
                null,
                null,
                partitionManager,
                dynamicPartitionOverwrite);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void assertReportedDirectories(
            FormatTablePartitionManager partitionManager,
            Path tablePath,
            List<Map<String, String>> expectedSpecs) {
        ArgumentCaptor<List<Map<String, String>>> specs =
                ArgumentCaptor.forClass((Class) List.class);
        ArgumentCaptor<List<Map<String, String>>> options =
                ArgumentCaptor.forClass((Class) List.class);
        verify(partitionManager)
                .createPartitions(
                        specs.capture(),
                        eq(true),
                        org.mockito.ArgumentMatchers.anyList(),
                        eq(true),
                        options.capture());
        assertThat(specs.getValue()).containsExactlyInAnyOrderElementsOf(expectedSpecs);
        assertThat(options.getValue()).hasSameSizeAs(specs.getValue());
        for (int i = 0; i < specs.getValue().size(); i++) {
            Map<String, String> spec = specs.getValue().get(i);
            assertThat(options.getValue().get(i))
                    .as("the directory named for %s keeps the escapes of its value", spec)
                    .isEqualTo(
                            Collections.singletonMap(
                                    CoreOptions.PATH.key(),
                                    new Path(tablePath, partitionDirectory(spec)).toString()));
        }
    }

    private static String partitionDirectory(Map<String, String> spec) {
        return PartitionPathUtils.generatePartitionPathUtil(new LinkedHashMap<>(spec), false);
    }

    private static Partition partitionAt(Map<String, String> spec, String location) {
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
                Collections.singletonMap(CoreOptions.PATH.key(), location));
    }
}
