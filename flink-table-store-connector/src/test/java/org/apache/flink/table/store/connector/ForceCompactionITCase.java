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

package org.apache.flink.table.store.connector;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.manifest.ManifestFileMeta;
import org.apache.flink.table.store.file.manifest.ManifestList;
import org.apache.flink.table.store.file.stats.BinaryTableStats;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.format.FileFormat;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for auto-enabling commit.force-compact under batch mode. */
public class ForceCompactionITCase extends CatalogITCaseBase {

    private final FileFormat avro = FileFormat.fromIdentifier("avro", new Configuration());

    @Override
    protected List<String> ddl() {
        return Arrays.asList(
                "CREATE TABLE IF NOT EXISTS T (\n"
                        + "  f0 INT\n, "
                        + "  f1 STRING\n, "
                        + "  f2 STRING\n"
                        + ") PARTITIONED BY (f1)",
                "CREATE TABLE IF NOT EXISTS T1 (\n"
                        + "  f0 INT\n, "
                        + "  f1 STRING\n, "
                        + "  f2 STRING\n"
                        + ")",
                "CREATE TABLE IF NOT EXISTS T2 (\n"
                        + "  f0 INT\n, "
                        + "  f1 STRING\n, "
                        + "  f2 STRING\n"
                        + ") WITH (\n"
                        + "'write-mode' = 'append-only')");
    }

    @Test
    public void testDynamicPartition() {
        batchSql("ALTER TABLE T SET ('num-levels' = '3')");

        // Winter: 1, Spring: 1, Summer: 1
        assertAppend(
                "INSERT INTO T VALUES(1, 'Winter', 'Winter is Coming'),"
                        + "(2, 'Winter', 'The First Snowflake'), "
                        + "(2, 'Spring', 'The First Rose in Spring'), "
                        + "(7, 'Summer', 'Summertime Sadness')",
                "T",
                1L);

        // Winter: 2
        assertAppend("INSERT INTO T VALUES(12, 'Winter', 'Last Christmas')", "T", 2L);

        // Winter: 3, Spring: 2
        assertAppend(
                "INSERT INTO T VALUES(11, 'Winter', 'Winter is Coming'), "
                        + "(4, 'Spring', 'April')",
                "T",
                3L);

        // Autumn: 1
        assertAppend("INSERT INTO T VALUES(10, 'Autumn', 'Refrain')", "T", 4L);

        // Summer: 2, Spring: 3
        assertAppend(
                "INSERT INTO T VALUES(6, 'Summer', 'Watermelon Sugar'), "
                        + "(4, 'Spring', 'Spring Water')",
                "T",
                5L);

        // Summer: 3, Autumn: 2
        assertAppend(
                "INSERT INTO T VALUES(66, 'Summer', 'Summer Vibe'),"
                        + " (9, 'Autumn', 'Wake Me Up When September Ends')",
                "T",
                6L);

        // Summer: 4, Autumn: 3
        assertAppend(
                "INSERT INTO T VALUES(666, 'Summer', 'Summer Vibe'),"
                        + " (9, 'Autumn', 'Wake Me Up When September Ends')",
                "T",
                7L);

        // Summer: 5, Autumn: 4, trigger compaction for partition 'Summer'
        assertCompact(
                "INSERT INTO T VALUES(6666, 'Summer', 'Summer Vibe'),"
                        + " (9, 'Autumn', 'Wake Me Up When September Ends')",
                "T",
                9L,
                "Summer",
                "Summer");

        // Summer: 6, Autumn: 5, trigger compaction for partition 'Autumn'
        assertCompact(
                "INSERT INTO T VALUES(66666, 'Summer', 'Summer Vibe'),"
                        + " (9, 'Autumn', 'Wake Me Up When September Ends')",
                "T",
                11L,
                "Autumn",
                "Autumn");

        // Winter: 4, Spring: 4
        assertAppend(
                "INSERT INTO T VALUES(1, 'Winter', 'Cold Water'), (4, 'Spring', 'SpringBoot')",
                "T",
                12L);

        // Winter: 5, Spring: 5, trigger compaction for both partitions
        assertCompact(
                "INSERT INTO T VALUES(1, 'Winter', 'Winter is Coming'),"
                        + " (4, 'Spring', 'The First Rose in Spring')",
                "T",
                14L,
                "Spring",
                "Winter");

        assertThat(batchSql("SELECT * FROM T")).hasSize(22);
    }

    @Test
    public void testNoDefaultNumOfLevels() {
        assertAppend(
                "INSERT INTO T1 VALUES(1, 'Winter', 'Winter is Coming'),"
                        + "(2, 'Winter', 'The First Snowflake'), "
                        + "(2, 'Spring', 'The First Rose in Spring'), "
                        + "(7, 'Summer', 'Summertime Sadness')",
                "T1",
                1L);

        assertAppend("INSERT INTO T1 VALUES(12, 'Winter', 'Last Christmas')", "T1", 2L);

        assertAppend("INSERT INTO T1 VALUES(11, 'Winter', 'Winter is Coming')", "T1", 3L);

        assertAppend("INSERT INTO T1 VALUES(10, 'Autumn', 'Refrain')", "T1", 4L);

        assertCompact(
                "INSERT INTO T1 VALUES(6, 'Summer', 'Watermelon Sugar'), "
                        + "(4, 'Spring', 'Spring Water')",
                "T1",
                6L);

        assertAppend(
                "INSERT INTO T1 VALUES(66, 'Summer', 'Summer Vibe'), "
                        + "(9, 'Autumn', 'Wake Me Up When September Ends')",
                "T1",
                7L);

        assertAppend(
                "INSERT INTO T1 VALUES(666, 'Summer', 'Summer Vibe'), "
                        + "(9, 'Autumn', 'Wake Me Up When September Ends')",
                "T1",
                8L);

        // change num-sorted-run.compaction-trigger to test restore LSM
        batchSql("ALTER TABLE T1 SET ('num-sorted-run.compaction-trigger' = '2')");

        assertCompact(
                "INSERT INTO T1 VALUES(666, 'Summer', 'Summer Vibe'), "
                        + "(9, 'Autumn', 'Wake Me Up When September Ends')",
                "T1",
                10L);

        assertThat(batchSql("SELECT * FROM T1")).hasSize(15);
    }

    @Test
    public void testForceCompact() {
        // explicit set false to verify force compact
        batchSql("ALTER TABLE T1 SET ('commit.force-compact' = 'false')");
        batchSql("ALTER TABLE T1 SET ('num-sorted-run.compaction-trigger' = '2')");

        assertAppend("INSERT INTO T1 VALUES (1, 'Winter', 'Winter is Coming')", "T1", 1L);
        assertAppend("INSERT INTO T1 VALUES (2, 'Spring', 'Spring Water')", "T1", 2L);
        assertCompact("INSERT INTO T1 VALUES (3, 'Summer', 'Summer Vibe')", "T1", 4L);

        batchSql("ALTER TABLE T2 SET ('commit.force-compact' = 'false')");
        batchSql("ALTER TABLE T2 SET ('compaction.early-max.file-num' = '2')");
        assertAppend("INSERT INTO T2 VALUES (1, 'Winter', 'Winter is Coming')", "T2", 1L);
        assertCompact("INSERT INTO T2 VALUES (2, 'Spring', 'Spring Water')", "T2", 3L);
    }

    private void assertAppend(String sql, String tableName, long expectSnapshotId) {
        batchSql(sql);
        Snapshot snapshot = findLatestSnapshot(tableName);
        assertThat(snapshot.id()).isEqualTo(expectSnapshotId);
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);
    }

    private void assertCompact(
            String sql, String tableName, long expectSnapshotId, String... expectPartMinMax) {
        batchSql(sql);
        Snapshot snapshot = findLatestSnapshot(tableName);
        assertThat(snapshot.id()).isEqualTo(expectSnapshotId);
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);
        RowType partType =
                expectPartMinMax.length > 0
                        ? RowType.of(new LogicalType[] {new VarCharType()}, new String[] {"f1"})
                        : RowType.of();
        FileStorePathFactory pathFactory =
                new FileStorePathFactory(
                        getTableDirectory(tableName),
                        partType,
                        "default",
                        CoreOptions.FILE_FORMAT.defaultValue());

        List<ManifestFileMeta> manifestFileMetas =
                new ManifestList.Factory(partType, avro, pathFactory)
                        .create()
                        .read(snapshot.deltaManifestList());
        assertThat(manifestFileMetas.get(0).numDeletedFiles()).isGreaterThanOrEqualTo(1);
        BinaryTableStats partStats = manifestFileMetas.get(0).partitionStats();
        if (expectPartMinMax.length > 0) {
            assertThat(partStats.min().getString(0).toString()).isEqualTo(expectPartMinMax[0]);
            assertThat(partStats.max().getString(0).toString()).isEqualTo(expectPartMinMax[1]);
        }
    }
}
