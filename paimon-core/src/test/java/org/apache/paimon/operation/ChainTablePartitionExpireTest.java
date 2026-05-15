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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.PartitionModification;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ChainTablePartitionExpire}. */
public class ChainTablePartitionExpireTest {

    @TempDir java.nio.file.Path tempDir;

    private String commitUser;

    @BeforeEach
    public void before() {
        commitUser = UUID.randomUUID().toString();
    }

    @Test
    public void testExpireWithSinglePartitionKey() throws Exception {
        Path tablePath = tablePath("simple_expire");
        createChainTable(tablePath, false);
        FileStoreTable mainTable = loadTable(tablePath);
        FileStoreTable snapshotTable = mainTable.switchToBranch("snapshot");
        FileStoreTable deltaTable = mainTable.switchToBranch("delta");

        write(snapshotTable, "20250101", "v1");
        write(snapshotTable, "20250201", "v2");
        write(snapshotTable, "20250301", "v3");

        write(deltaTable, "20250110", "v4");
        write(deltaTable, "20250115", "v5");
        write(deltaTable, "20250210", "v6");
        write(deltaTable, "20250215", "v7");
        write(deltaTable, "20250315", "v8");

        snapshotTable = loadTable(tablePath).switchToBranch("snapshot");
        deltaTable = loadTable(tablePath).switchToBranch("delta");
        assertThat(listPartitions(snapshotTable))
                .containsExactlyInAnyOrder("20250101", "20250201", "20250301");
        assertThat(listPartitions(deltaTable))
                .containsExactlyInAnyOrder(
                        "20250110", "20250115", "20250210", "20250215", "20250315");

        // cutoff = 2025-03-31 - 20d = 2025-03-11
        // Snapshots before cutoff: 20250101, 20250201, 20250301 (3 snapshots)
        // Anchor = 20250301 (kept), expire 2 segments:
        //   Segment0: S(20250101), d(20250110), d(20250115)
        //   Segment1: S(20250201), d(20250210), d(20250215)
        ChainTablePartitionExpire expire =
                newChainExpire(snapshotTable, deltaTable, Duration.ofDays(20), false);
        expire.setLastCheck(LocalDateTime.of(2025, 1, 1, 0, 0));
        List<Map<String, String>> expired =
                expire.expire(LocalDateTime.of(2025, 3, 31, 0, 0), Long.MAX_VALUE);

        assertThat(expired).isNotNull();
        assertThat(expired).isNotEmpty();

        snapshotTable = loadTable(tablePath).switchToBranch("snapshot");
        deltaTable = loadTable(tablePath).switchToBranch("delta");
        assertThat(listPartitions(snapshotTable)).containsExactlyInAnyOrder("20250301");
        assertThat(listPartitions(deltaTable)).containsExactlyInAnyOrder("20250315");
    }

    @Test
    public void testNoExpireWhenOnlyOneSnapshotBeforeCutoff() throws Exception {
        Path tablePath = tablePath("no_expire_one_snapshot");
        createChainTable(tablePath, false);
        FileStoreTable mainTable = loadTable(tablePath);
        FileStoreTable snapshotTable = mainTable.switchToBranch("snapshot");
        FileStoreTable deltaTable = mainTable.switchToBranch("delta");

        write(snapshotTable, "20250201", "v1");
        write(snapshotTable, "20250315", "v2");
        write(deltaTable, "20250205", "v3");

        snapshotTable = loadTable(tablePath).switchToBranch("snapshot");
        deltaTable = loadTable(tablePath).switchToBranch("delta");

        ChainTablePartitionExpire expire =
                newChainExpire(snapshotTable, deltaTable, Duration.ofDays(30), false);
        expire.setLastCheck(LocalDateTime.of(2025, 1, 1, 0, 0));
        List<Map<String, String>> expired =
                expire.expire(LocalDateTime.of(2025, 3, 31, 0, 0), Long.MAX_VALUE);

        assertThat(expired).isEmpty();

        snapshotTable = loadTable(tablePath).switchToBranch("snapshot");
        deltaTable = loadTable(tablePath).switchToBranch("delta");
        assertThat(listPartitions(snapshotTable)).containsExactlyInAnyOrder("20250201", "20250315");
        assertThat(listPartitions(deltaTable)).containsExactlyInAnyOrder("20250205");
    }

    @Test
    public void testExpireMultipleSegments() throws Exception {
        Path tablePath = tablePath("multi_segments");
        createChainTable(tablePath, false);
        FileStoreTable mainTable = loadTable(tablePath);
        FileStoreTable snapshotTable = mainTable.switchToBranch("snapshot");
        FileStoreTable deltaTable = mainTable.switchToBranch("delta");

        write(snapshotTable, "20250101", "v1");
        write(snapshotTable, "20250115", "v2");
        write(snapshotTable, "20250201", "v3");
        write(snapshotTable, "20250315", "v4");

        write(deltaTable, "20250105", "v5");
        write(deltaTable, "20250120", "v6");
        write(deltaTable, "20250210", "v7");

        snapshotTable = loadTable(tablePath).switchToBranch("snapshot");
        deltaTable = loadTable(tablePath).switchToBranch("delta");

        // cutoff = 2025-03-31 - 30d = 2025-03-01
        // Snapshots before cutoff: 20250101, 20250115, 20250201 (3 snapshots)
        // Anchor = 20250201 (kept), expire S(20250101), S(20250115)
        // Delta before anchor: d(20250105), d(20250120)
        ChainTablePartitionExpire expire =
                newChainExpire(snapshotTable, deltaTable, Duration.ofDays(30), false);
        expire.setLastCheck(LocalDateTime.of(2025, 1, 1, 0, 0));
        expire.expire(LocalDateTime.of(2025, 3, 31, 0, 0), Long.MAX_VALUE);

        snapshotTable = loadTable(tablePath).switchToBranch("snapshot");
        deltaTable = loadTable(tablePath).switchToBranch("delta");
        assertThat(listPartitions(snapshotTable)).containsExactlyInAnyOrder("20250201", "20250315");
        assertThat(listPartitions(deltaTable)).containsExactlyInAnyOrder("20250210");
    }

    @Test
    public void testNoExpireWhenNoSnapshotsBeforeCutoff() throws Exception {
        Path tablePath = tablePath("no_expire_no_snapshot");
        createChainTable(tablePath, false);
        FileStoreTable mainTable = loadTable(tablePath);
        FileStoreTable snapshotTable = mainTable.switchToBranch("snapshot");
        FileStoreTable deltaTable = mainTable.switchToBranch("delta");

        write(snapshotTable, "20250315", "v1");
        write(snapshotTable, "20250320", "v2");
        write(deltaTable, "20250316", "v3");

        snapshotTable = loadTable(tablePath).switchToBranch("snapshot");
        deltaTable = loadTable(tablePath).switchToBranch("delta");

        // cutoff = 2025-03-31 - 30d = 2025-03-01
        // No snapshots before cutoff
        ChainTablePartitionExpire expire =
                newChainExpire(snapshotTable, deltaTable, Duration.ofDays(30), false);
        expire.setLastCheck(LocalDateTime.of(2025, 1, 1, 0, 0));
        List<Map<String, String>> expired =
                expire.expire(LocalDateTime.of(2025, 3, 31, 0, 0), Long.MAX_VALUE);

        assertThat(expired).isEmpty();
    }

    @Test
    public void testCheckIntervalPreventsExpire() throws Exception {
        Path tablePath = tablePath("check_interval");
        createChainTable(tablePath, false);
        FileStoreTable mainTable = loadTable(tablePath);
        FileStoreTable snapshotTable = mainTable.switchToBranch("snapshot");
        FileStoreTable deltaTable = mainTable.switchToBranch("delta");

        write(snapshotTable, "20250101", "v1");
        write(snapshotTable, "20250201", "v2");

        snapshotTable = loadTable(tablePath).switchToBranch("snapshot");
        deltaTable = loadTable(tablePath).switchToBranch("delta");

        ChainTablePartitionExpire expire =
                new ChainTablePartitionExpire(
                        Duration.ofDays(30),
                        Duration.ofDays(1),
                        snapshotTable,
                        deltaTable,
                        CoreOptions.fromMap(buildOptions(Duration.ofDays(30), false)),
                        snapshotTable.schema().logicalPartitionType(),
                        false,
                        Integer.MAX_VALUE,
                        0,
                        null,
                        null);
        expire.setLastCheck(LocalDateTime.of(2025, 3, 31, 0, 0));

        List<Map<String, String>> expired =
                expire.expire(LocalDateTime.of(2025, 3, 31, 0, 0), Long.MAX_VALUE);

        assertThat(expired).isNull();
    }

    @Test
    public void testMaxExpireNumLimitsSegments() throws Exception {
        Path tablePath = tablePath("max_expire_num");
        createChainTable(tablePath, false);
        FileStoreTable mainTable = loadTable(tablePath);
        FileStoreTable snapshotTable = mainTable.switchToBranch("snapshot");
        FileStoreTable deltaTable = mainTable.switchToBranch("delta");

        // Snapshots: S(0101), S(0115), S(0201), S(0315)
        // cutoff = 03-01, anchor = S(0201)
        // Segments to expire: Segment1={S(0101), d(0105)}, Segment2={S(0115), d(0120)}
        write(snapshotTable, "20250101", "v1");
        write(snapshotTable, "20250115", "v2");
        write(snapshotTable, "20250201", "v3");
        write(snapshotTable, "20250315", "v4");

        write(deltaTable, "20250105", "v5");
        write(deltaTable, "20250120", "v6");
        write(deltaTable, "20250210", "v7");

        snapshotTable = loadTable(tablePath).switchToBranch("snapshot");
        deltaTable = loadTable(tablePath).switchToBranch("delta");

        // maxExpireNum=1 means only 1 segment: Segment1={S(0101), d(0105)}
        ChainTablePartitionExpire expire =
                new ChainTablePartitionExpire(
                        Duration.ofDays(30),
                        Duration.ZERO,
                        snapshotTable,
                        deltaTable,
                        CoreOptions.fromMap(buildOptions(Duration.ofDays(30), false)),
                        snapshotTable.schema().logicalPartitionType(),
                        false,
                        1,
                        0,
                        null,
                        null);
        expire.setLastCheck(LocalDateTime.of(2025, 1, 1, 0, 0));
        List<Map<String, String>> expired =
                expire.expire(LocalDateTime.of(2025, 3, 31, 0, 0), Long.MAX_VALUE);

        assertThat(expired).isNotNull();
        // 1 segment = S(0101) + d(0105) = 2 partitions
        assertThat(expired).hasSize(2);

        // Verify: S(0101) expired, S(0115) still exists (not expired, was in segment 2)
        snapshotTable = loadTable(tablePath).switchToBranch("snapshot");
        deltaTable = loadTable(tablePath).switchToBranch("delta");
        assertThat(listPartitions(snapshotTable))
                .containsExactlyInAnyOrder("20250115", "20250201", "20250315");
        // d(0105) expired, d(0120) and d(0210) kept
        assertThat(listPartitions(deltaTable)).containsExactlyInAnyOrder("20250120", "20250210");
    }

    @Test
    public void testExpireWithGroupPartition() throws Exception {
        Path tablePath = tablePath("group_partition");
        createChainTable(tablePath, true);
        FileStoreTable mainTable = loadTable(tablePath);
        FileStoreTable snapshotTable = mainTable.switchToBranch("snapshot");
        FileStoreTable deltaTable = mainTable.switchToBranch("delta");

        // Group "US": snapshots 0101, 0201, 0301
        writeGrouped(snapshotTable, "US", "20250101", "v1");
        writeGrouped(snapshotTable, "US", "20250201", "v2");
        writeGrouped(snapshotTable, "US", "20250301", "v3");
        // Group "US": deltas 0110, 0210
        writeGrouped(deltaTable, "US", "20250110", "d1");
        writeGrouped(deltaTable, "US", "20250210", "d2");

        // Group "EU": only one snapshot before cutoff, so nothing should expire
        writeGrouped(snapshotTable, "EU", "20250215", "v4");
        writeGrouped(snapshotTable, "EU", "20250320", "v5");
        writeGrouped(deltaTable, "EU", "20250220", "d3");

        snapshotTable = loadTable(tablePath).switchToBranch("snapshot");
        deltaTable = loadTable(tablePath).switchToBranch("delta");

        // cutoff = 2025-03-31 - 30d = 2025-03-01
        // Group "US": snapshots before cutoff = [0101, 0201]. Anchor = 0201 (kept).
        //   Expire: S(0101), delta(0110) (before anchor 0201)
        //   Keep: S(0201), S(0301), delta(0210)
        // Group "EU": snapshots before cutoff = [0215] (only 1). Nothing expired.
        ChainTablePartitionExpire expire =
                newChainExpire(snapshotTable, deltaTable, Duration.ofDays(30), true);
        expire.setLastCheck(LocalDateTime.of(2025, 1, 1, 0, 0));
        List<Map<String, String>> expired =
                expire.expire(LocalDateTime.of(2025, 3, 31, 0, 0), Long.MAX_VALUE);

        assertThat(expired).isNotNull();
        assertThat(expired).hasSize(2);

        snapshotTable = loadTable(tablePath).switchToBranch("snapshot");
        deltaTable = loadTable(tablePath).switchToBranch("delta");

        List<String> snapshotParts = listGroupedPartitions(snapshotTable);
        List<String> deltaParts = listGroupedPartitions(deltaTable);

        assertThat(snapshotParts).contains("US|20250201", "US|20250301");
        assertThat(snapshotParts).doesNotContain("US|20250101");
        assertThat(snapshotParts).contains("EU|20250215", "EU|20250320");

        assertThat(deltaParts).contains("US|20250210");
        assertThat(deltaParts).doesNotContain("US|20250110");
        assertThat(deltaParts).contains("EU|20250220");
    }

    @Test
    public void testUsesBranchSpecificPartitionModifications() throws Exception {
        Path tablePath = tablePath("branch_specific_partition_modification");
        createChainTable(tablePath, false);
        FileStoreTable mainTable = loadTable(tablePath);
        FileStoreTable snapshotTable = mainTable.switchToBranch("snapshot");
        FileStoreTable deltaTable = mainTable.switchToBranch("delta");

        write(snapshotTable, "20250101", "v1");
        write(snapshotTable, "20250201", "v2");
        write(snapshotTable, "20250301", "v3");
        write(deltaTable, "20250110", "d1");
        write(deltaTable, "20250210", "d2");

        snapshotTable = loadTable(tablePath).switchToBranch("snapshot");
        deltaTable = loadTable(tablePath).switchToBranch("delta");

        RecordingPartitionModification snapshotModification = new RecordingPartitionModification();
        RecordingPartitionModification deltaModification = new RecordingPartitionModification();

        ChainTablePartitionExpire expire =
                new ChainTablePartitionExpire(
                        Duration.ofDays(30),
                        Duration.ZERO,
                        snapshotTable,
                        deltaTable,
                        CoreOptions.fromMap(buildOptions(Duration.ofDays(30), false)),
                        snapshotTable.schema().logicalPartitionType(),
                        false,
                        Integer.MAX_VALUE,
                        0,
                        snapshotModification,
                        deltaModification);
        expire.setLastCheck(LocalDateTime.of(2025, 1, 1, 0, 0));
        expire.expire(LocalDateTime.of(2025, 3, 31, 0, 0), Long.MAX_VALUE);

        assertThat(deltaModification.droppedValues("dt"))
                .contains("20250110", "20250110.done")
                .doesNotContain("20250101", "20250101.done");
        assertThat(snapshotModification.droppedValues("dt"))
                .contains("20250101", "20250101.done")
                .doesNotContain("20250110", "20250110.done");
    }

    @Test
    public void testIsValueAllExpiredReturnsFalseForAnchor() throws Exception {
        Path tablePath = tablePath("value_expired_anchor");
        createChainTable(tablePath, false);
        FileStoreTable mainTable = loadTable(tablePath);
        FileStoreTable snapshotTable = mainTable.switchToBranch("snapshot");
        FileStoreTable deltaTable = mainTable.switchToBranch("delta");

        // S(0101), S(0201), S(0301)
        write(snapshotTable, "20250101", "v1");
        write(snapshotTable, "20250201", "v2");
        write(snapshotTable, "20250301", "v3");

        snapshotTable = loadTable(tablePath).switchToBranch("snapshot");
        deltaTable = loadTable(tablePath).switchToBranch("delta");

        // expirationTime = 30d, "now" = 2025-03-31 → cutoff = 2025-03-01
        // Snapshots before cutoff: S(0101), S(0201). Anchor = S(0201) (kept).
        ChainTablePartitionExpire expire =
                newChainExpire(snapshotTable, deltaTable, Duration.ofDays(30), false);
        LocalDateTime now = LocalDateTime.of(2025, 3, 31, 0, 0);

        BinaryRow anchor0201 = findPartition(snapshotTable, "20250201");
        BinaryRow expired0101 = findPartition(snapshotTable, "20250101");

        // Anchor partition alone → not all expired (anchor is retained)
        assertThat(expire.isValueAllExpired(Collections.singletonList(anchor0201), now)).isFalse();

        // Truly expired partition alone → all expired
        assertThat(expire.isValueAllExpired(Collections.singletonList(expired0101), now)).isTrue();

        // Mix of anchor + expired → not all expired
        assertThat(expire.isValueAllExpired(Arrays.asList(expired0101, anchor0201), now)).isFalse();
    }

    @Test
    public void testIsValueAllExpiredReturnsFalseWhenTooFewSnapshots() throws Exception {
        Path tablePath = tablePath("value_expired_few_snapshots");
        createChainTable(tablePath, false);
        FileStoreTable mainTable = loadTable(tablePath);
        FileStoreTable snapshotTable = mainTable.switchToBranch("snapshot");
        FileStoreTable deltaTable = mainTable.switchToBranch("delta");

        // Only 1 snapshot before cutoff
        write(snapshotTable, "20250201", "v1");
        write(snapshotTable, "20250315", "v2");

        snapshotTable = loadTable(tablePath).switchToBranch("snapshot");
        deltaTable = loadTable(tablePath).switchToBranch("delta");

        // cutoff = 2025-03-31 - 30d = 2025-03-01
        // Only S(0201) before cutoff → < 2, nothing can expire
        ChainTablePartitionExpire expire =
                newChainExpire(snapshotTable, deltaTable, Duration.ofDays(30), false);
        LocalDateTime now = LocalDateTime.of(2025, 3, 31, 0, 0);

        BinaryRow partition0201 = findPartition(snapshotTable, "20250201");
        assertThat(expire.isValueAllExpired(Collections.singletonList(partition0201), now))
                .isFalse();
    }

    @Test
    public void testIsValueAllExpiredReturnsFalseForDeltaOnlyGroup() throws Exception {
        Path tablePath = tablePath("value_expired_delta_only");
        createChainTable(tablePath, false);
        FileStoreTable mainTable = loadTable(tablePath);
        FileStoreTable snapshotTable = mainTable.switchToBranch("snapshot");
        FileStoreTable deltaTable = mainTable.switchToBranch("delta");

        write(deltaTable, "20250101", "d1");
        write(deltaTable, "20250201", "d2");

        snapshotTable = loadTable(tablePath).switchToBranch("snapshot");
        deltaTable = loadTable(tablePath).switchToBranch("delta");

        // cutoff = 2025-03-31 - 30d = 2025-03-01
        // No snapshot boundary exists, so delta-only partitions are retained.
        ChainTablePartitionExpire expire =
                newChainExpire(snapshotTable, deltaTable, Duration.ofDays(30), false);
        LocalDateTime now = LocalDateTime.of(2025, 3, 31, 0, 0);

        BinaryRow deltaPartition = findPartition(deltaTable, "20250101");
        assertThat(expire.isValueAllExpired(Collections.singletonList(deltaPartition), now))
                .isFalse();
    }

    @Test
    public void testIsValueAllExpiredWithGroupPartitions() throws Exception {
        Path tablePath = tablePath("value_expired_groups");
        createChainTable(tablePath, true);
        FileStoreTable mainTable = loadTable(tablePath);
        FileStoreTable snapshotTable = mainTable.switchToBranch("snapshot");
        FileStoreTable deltaTable = mainTable.switchToBranch("delta");

        // Group "US": 3 snapshots, anchor = S(US,0201)
        writeGrouped(snapshotTable, "US", "20250101", "v1");
        writeGrouped(snapshotTable, "US", "20250201", "v2");
        writeGrouped(snapshotTable, "US", "20250301", "v3");

        // Group "EU": only 1 snapshot before cutoff → nothing expires
        writeGrouped(snapshotTable, "EU", "20250215", "v4");
        writeGrouped(snapshotTable, "EU", "20250320", "v5");

        snapshotTable = loadTable(tablePath).switchToBranch("snapshot");
        deltaTable = loadTable(tablePath).switchToBranch("delta");

        // cutoff = 2025-03-31 - 30d = 2025-03-01
        ChainTablePartitionExpire expire =
                newChainExpire(snapshotTable, deltaTable, Duration.ofDays(30), true);
        LocalDateTime now = LocalDateTime.of(2025, 3, 31, 0, 0);

        BinaryRow usExpired = findGroupedPartition(snapshotTable, "US", "20250101");
        BinaryRow usAnchor = findGroupedPartition(snapshotTable, "US", "20250201");
        BinaryRow euRetained = findGroupedPartition(snapshotTable, "EU", "20250215");

        // US expired partition → truly expired
        assertThat(expire.isValueAllExpired(Collections.singletonList(usExpired), now)).isTrue();

        // US anchor → retained
        assertThat(expire.isValueAllExpired(Collections.singletonList(usAnchor), now)).isFalse();

        // EU partition (< 2 snapshots before cutoff) → retained
        assertThat(expire.isValueAllExpired(Collections.singletonList(euRetained), now)).isFalse();

        // Mix across groups: US expired + EU retained
        assertThat(expire.isValueAllExpired(Arrays.asList(usExpired, euRetained), now)).isFalse();
    }

    @Test
    public void testIsValueAllExpiredReturnsFalseForPartitionsAfterCutoff() throws Exception {
        Path tablePath = tablePath("value_expired_after_cutoff");
        createChainTable(tablePath, false);
        FileStoreTable mainTable = loadTable(tablePath);
        FileStoreTable snapshotTable = mainTable.switchToBranch("snapshot");
        FileStoreTable deltaTable = mainTable.switchToBranch("delta");

        write(snapshotTable, "20250101", "v1");
        write(snapshotTable, "20250315", "v2");

        snapshotTable = loadTable(tablePath).switchToBranch("snapshot");
        deltaTable = loadTable(tablePath).switchToBranch("delta");

        // cutoff = 2025-03-31 - 30d = 2025-03-01
        // S(0315) is after cutoff → not expired at all
        ChainTablePartitionExpire expire =
                newChainExpire(snapshotTable, deltaTable, Duration.ofDays(30), false);
        LocalDateTime now = LocalDateTime.of(2025, 3, 31, 0, 0);

        BinaryRow afterCutoff = findPartition(snapshotTable, "20250315");
        assertThat(expire.isValueAllExpired(Collections.singletonList(afterCutoff), now)).isFalse();
    }

    // ========== Helper methods ==========

    private BinaryRow findPartition(FileStoreTable table, String dtValue) {
        return table.newSnapshotReader().partitionEntries().stream()
                .map(PartitionEntry::partition)
                .filter(p -> p.getString(0).toString().equals(dtValue))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Partition " + dtValue + " not found"));
    }

    private BinaryRow findGroupedPartition(FileStoreTable table, String region, String dt) {
        return table.newSnapshotReader().partitionEntries().stream()
                .map(PartitionEntry::partition)
                .filter(
                        p ->
                                p.getString(0).toString().equals(region)
                                        && p.getString(1).toString().equals(dt))
                .findFirst()
                .orElseThrow(
                        () ->
                                new RuntimeException(
                                        "Partition " + region + "|" + dt + " not found"));
    }

    private Path tablePath(String tableName) {
        return new Path(tempDir.toUri().toString(), tableName);
    }

    private void createChainTable(Path tablePath, boolean withGroupPartition) throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        SchemaManager schemaManager = new SchemaManager(fileIO, tablePath);

        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "1");
        options.put("merge-engine", "deduplicate");
        options.put("sequence.field", "v");

        Schema schema;
        if (withGroupPartition) {
            schema =
                    new Schema(
                            RowType.of(
                                            new org.apache.paimon.types.DataType[] {
                                                DataTypes.STRING(),
                                                DataTypes.STRING(),
                                                DataTypes.STRING(),
                                                DataTypes.STRING()
                                            },
                                            new String[] {"region", "dt", "pk", "v"})
                                    .getFields(),
                            Arrays.asList("region", "dt"),
                            Arrays.asList("pk", "region", "dt"),
                            options,
                            "");
        } else {
            schema =
                    new Schema(
                            RowType.of(
                                            new org.apache.paimon.types.DataType[] {
                                                DataTypes.STRING(),
                                                DataTypes.STRING(),
                                                DataTypes.STRING()
                                            },
                                            new String[] {"dt", "pk", "v"})
                                    .getFields(),
                            Collections.singletonList("dt"),
                            Arrays.asList("pk", "dt"),
                            options,
                            "");
        }
        schemaManager.createTable(schema);

        FileStoreTable mainTable = loadTable(tablePath);
        mainTable.createBranch("snapshot");
        mainTable.createBranch("delta");

        List<SchemaChange> chainTableOptions =
                Arrays.asList(
                        SchemaChange.setOption("chain-table.enabled", "true"),
                        SchemaChange.setOption("scan.fallback-snapshot-branch", "snapshot"),
                        SchemaChange.setOption("scan.fallback-delta-branch", "delta"),
                        SchemaChange.setOption("partition.timestamp-pattern", "$dt"),
                        SchemaChange.setOption("partition.timestamp-formatter", "yyyyMMdd"));
        if (withGroupPartition) {
            chainTableOptions = new java.util.ArrayList<>(chainTableOptions);
            chainTableOptions.add(SchemaChange.setOption("chain-table.chain-partition-keys", "dt"));
        }
        schemaManager.commitChanges(chainTableOptions);
        new SchemaManager(fileIO, tablePath, "snapshot").commitChanges(chainTableOptions);
        new SchemaManager(fileIO, tablePath, "delta").commitChanges(chainTableOptions);
    }

    private FileStoreTable loadTable(Path tablePath) {
        LocalFileIO fileIO = LocalFileIO.create();
        Options options = new Options();
        options.set(CoreOptions.PATH, tablePath.toString());
        String branchName = CoreOptions.branch(options.toMap());
        TableSchema tableSchema = new SchemaManager(fileIO, tablePath, branchName).latest().get();
        return FileStoreTableFactory.create(
                fileIO, tablePath, tableSchema, CatalogEnvironment.empty());
    }

    private void write(FileStoreTable table, String dt, String v) throws Exception {
        StreamTableWrite write =
                table.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"))
                        .newWrite(commitUser);
        write.write(
                GenericRow.of(
                        BinaryString.fromString(dt),
                        BinaryString.fromString(v),
                        BinaryString.fromString(v)));
        TableCommitImpl commit = table.newCommit(commitUser);
        List<CommitMessage> commitMessages = write.prepareCommit(true, 0);
        commit.commit(0, commitMessages);
        write.close();
        commit.close();
    }

    private void writeGrouped(FileStoreTable table, String region, String dt, String v)
            throws Exception {
        StreamTableWrite write =
                table.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"))
                        .newWrite(commitUser);
        write.write(
                GenericRow.of(
                        BinaryString.fromString(region),
                        BinaryString.fromString(dt),
                        BinaryString.fromString(v),
                        BinaryString.fromString(v)));
        TableCommitImpl commit = table.newCommit(commitUser);
        List<CommitMessage> commitMessages = write.prepareCommit(true, 0);
        commit.commit(0, commitMessages);
        write.close();
        commit.close();
    }

    private List<String> listPartitions(FileStoreTable table) {
        return table.newSnapshotReader().partitionEntries().stream()
                .map(PartitionEntry::partition)
                .map(p -> p.getString(0).toString())
                .sorted()
                .collect(Collectors.toList());
    }

    private List<String> listGroupedPartitions(FileStoreTable table) {
        return table.newSnapshotReader().partitionEntries().stream()
                .map(PartitionEntry::partition)
                .map(p -> p.getString(0).toString() + "|" + p.getString(1).toString())
                .sorted()
                .collect(Collectors.toList());
    }

    private Map<String, String> buildOptions(Duration expirationTime, boolean withGroupPartition) {
        Map<String, String> opts = new HashMap<>();
        opts.put("partition.timestamp-pattern", "$dt");
        opts.put("partition.timestamp-formatter", "yyyyMMdd");
        opts.put("scan.fallback-snapshot-branch", "snapshot");
        opts.put("scan.fallback-delta-branch", "delta");
        opts.put(CoreOptions.PARTITION_EXPIRATION_TIME.key(), expirationTime.toDays() + " d");
        if (withGroupPartition) {
            opts.put("chain-table.chain-partition-keys", "dt");
        }
        return opts;
    }

    private ChainTablePartitionExpire newChainExpire(
            FileStoreTable snapshotTable,
            FileStoreTable deltaTable,
            Duration expirationTime,
            boolean withGroupPartition) {
        return new ChainTablePartitionExpire(
                expirationTime,
                Duration.ZERO,
                snapshotTable,
                deltaTable,
                CoreOptions.fromMap(buildOptions(expirationTime, withGroupPartition)),
                snapshotTable.schema().logicalPartitionType(),
                false,
                Integer.MAX_VALUE,
                0,
                null,
                null);
    }

    private static class RecordingPartitionModification implements PartitionModification {

        private final List<Map<String, String>> droppedPartitions = new ArrayList<>();

        @Override
        public void createPartitions(List<Map<String, String>> partitions)
                throws Catalog.TableNotExistException {}

        @Override
        public void dropPartitions(List<Map<String, String>> partitions)
                throws Catalog.TableNotExistException {
            for (Map<String, String> partition : partitions) {
                droppedPartitions.add(new HashMap<>(partition));
            }
        }

        @Override
        public void alterPartitions(List<PartitionStatistics> partitions)
                throws Catalog.TableNotExistException {}

        @Override
        public void close() throws Exception {}

        private List<String> droppedValues(String key) {
            return droppedPartitions.stream()
                    .map(partition -> partition.get(key))
                    .collect(Collectors.toList());
        }
    }
}
