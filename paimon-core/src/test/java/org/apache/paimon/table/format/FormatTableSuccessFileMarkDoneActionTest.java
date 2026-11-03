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
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.partition.actions.PartitionMarkDoneAction;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.PartitionPathUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.CoreOptions.FORMAT_TABLE_PARTITION_ONLY_VALUE_IN_PATH;
import static org.apache.paimon.CoreOptions.PARTITION_MARK_DONE_ACTION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link FormatTableSuccessFileMarkDoneAction}. */
class FormatTableSuccessFileMarkDoneActionTest {

    @TempDir java.nio.file.Path tempDir;

    private final LocalFileIO fileIO = LocalFileIO.create();

    @Test
    void testWritesAnEmptySuccessFile() throws Exception {
        FormatTable table = table(Collections.emptyMap(), null);
        Path successPath = path(table, "dt=20240101/hh=08/_SUCCESS");
        fileIO.overwriteFileUtf8(successPath, "{\"creationTime\": 1}");

        markDone(table, "dt=20240101/hh=08");

        assertThat(fileIO.getFileStatus(successPath).getLen()).isZero();
    }

    @Test
    void testValueOnlyAndEscapedDirectories() throws Exception {
        FormatTable table =
                table(
                        Collections.singletonMap(
                                FORMAT_TABLE_PARTITION_ONLY_VALUE_IN_PATH.key(), "true"),
                        null);

        LinkedHashMap<String, String> spec = new LinkedHashMap<>(spec("a/b"));

        markDone(table, PartitionPathUtils.generatePartitionPath(spec));

        Path partitionPath =
                FormatTablePartitionPathResolver.defaultPartitionPath(
                        new Path(table.location()), spec, true);
        assertThat(partitionPath.getName()).isEqualTo("a%2Fb");
        assertThat(fileIO.exists(new Path(partitionPath, "_SUCCESS"))).isTrue();
    }

    @Test
    void testCatalogManagedPartitionsAreLookedUpTogether() throws Exception {
        String customLocation = tempDir.resolve("elsewhere").toUri().toString();
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        when(partitionManager.listPartitionsByNames(
                        Arrays.asList(spec("08"), spec("09"), spec("10"))))
                .thenReturn(
                        Arrays.asList(
                                partition(spec("09"), customLocation),
                                partition(spec("08"), null)));
        FormatTable table = table(Collections.emptyMap(), partitionManager);

        markDone(
                table,
                "dt=20240101/hh=08",
                "dt=20240101/hh=09",
                "dt=20240101/hh=10",
                "dt=20240101/hh=08");

        verify(partitionManager, times(1)).listPartitionsByNames(anyList());
        assertThat(fileIO.exists(path(table, "dt=20240101/hh=08/_SUCCESS"))).isTrue();
        // Custom location and unregistered partitions are skipped.
        assertThat(fileIO.exists(new Path(customLocation, "_SUCCESS"))).isFalse();
        assertThat(fileIO.exists(path(table, "dt=20240101/hh=09"))).isFalse();
        assertThat(fileIO.exists(path(table, "dt=20240101/hh=10"))).isFalse();
    }

    @Test
    void testInvalidArguments() {
        FormatTable table = table(Collections.emptyMap(), null);
        assertThatThrownBy(() -> markDone(table, "hh=08/dt=20240101"))
                .hasMessageContaining("does not name the partition keys [dt, hh]");

        FormatTable markEvent =
                table(
                        Collections.singletonMap(PARTITION_MARK_DONE_ACTION.key(), "mark-event"),
                        null);
        assertThatThrownBy(() -> createActions(markEvent))
                .hasMessageContaining("only supports the 'success-file' partition mark done");

        FormatTable unpartitioned =
                FormatTable.builder()
                        .fileIO(fileIO)
                        .identifier(Identifier.create("db", "events"))
                        .rowType(RowType.builder().field("id", DataTypes.INT()).build())
                        .partitionKeys(Collections.emptyList())
                        .location(tempDir.toUri().toString())
                        .format(FormatTable.Format.CSV)
                        .options(Collections.emptyMap())
                        .build();
        assertThatThrownBy(() -> createActions(unpartitioned))
                .hasMessageContaining("not partitioned");
    }

    private void markDone(FormatTable table, String... partitions) throws Exception {
        for (PartitionMarkDoneAction action : createActions(table)) {
            action.markDone(Arrays.asList(partitions));
            action.close();
        }
    }

    private static List<PartitionMarkDoneAction> createActions(FormatTable table) {
        return PartitionMarkDoneAction.createActions(
                FormatTableSuccessFileMarkDoneActionTest.class.getClassLoader(),
                table,
                CoreOptions.fromMap(table.options()));
    }

    private FormatTable table(
            Map<String, String> options, @Nullable FormatTablePartitionManager partitionManager) {
        return FormatTable.builder()
                .fileIO(fileIO)
                .identifier(Identifier.create("db", "events"))
                .rowType(
                        RowType.builder()
                                .field("id", DataTypes.INT())
                                .field("dt", DataTypes.STRING())
                                .field("hh", DataTypes.STRING())
                                .build())
                .partitionKeys(Arrays.asList("dt", "hh"))
                .location(tempDir.resolve("events").toUri().toString())
                .format(FormatTable.Format.CSV)
                .options(new HashMap<>(options))
                .partitionManager(partitionManager)
                .build();
    }

    private static Path path(FormatTable table, String relative) {
        return new Path(table.location(), relative);
    }

    private static Map<String, String> spec(String hh) {
        LinkedHashMap<String, String> spec = new LinkedHashMap<>();
        spec.put("dt", "20240101");
        spec.put("hh", hh);
        return spec;
    }

    private static Partition partition(Map<String, String> spec, @Nullable String location) {
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
                location == null
                        ? null
                        : Collections.singletonMap(CoreOptions.PATH.key(), location));
    }
}
