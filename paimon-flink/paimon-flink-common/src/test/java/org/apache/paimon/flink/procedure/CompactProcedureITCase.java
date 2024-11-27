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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.Snapshot;
import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.utils.BlockingIterator;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.StringUtils;

import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** IT Case for {@link CompactProcedure}. */
public class CompactProcedureITCase extends CatalogITCaseBase {

    // ----------------------- Non-sort Compact -----------------------
    @Test
    public void testBatchCompact() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " k INT,"
                        + " v INT,"
                        + " hh INT,"
                        + " dt STRING,"
                        + " PRIMARY KEY (k, dt, hh) NOT ENFORCED"
                        + ") PARTITIONED BY (dt, hh) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'bucket' = '1'"
                        + ")");
        FileStoreTable table = paimonTable("T");

        sql(
                "INSERT INTO T VALUES (1, 100, 15, '20221208'), (1, 100, 16, '20221208'), (1, 100, 15, '20221209')");
        sql(
                "INSERT INTO T VALUES (2, 100, 15, '20221208'), (2, 100, 16, '20221208'), (2, 100, 15, '20221209')");

        checkLatestSnapshot(table, 2, Snapshot.CommitKind.APPEND);

        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        ThreadLocalRandom random = ThreadLocalRandom.current();
        if (random.nextBoolean()) {
            sql(
                    "CALL sys.compact(`table` => 'default.T', partitions => 'dt=20221208,hh=15;dt=20221209,hh=15')");
        } else {
            sql(
                    "CALL sys.compact(`table` => 'default.T', `where` => '(dt=20221208 and hh=15) or (dt=20221209 and hh=15)')");
        }

        checkLatestSnapshot(table, 3, Snapshot.CommitKind.COMPACT);

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        assertThat(splits.size()).isEqualTo(3);
        for (DataSplit split : splits) {
            if (split.partition().getInt(1) == 15) {
                // compacted
                assertThat(split.dataFiles().size()).isEqualTo(1);
            } else {
                // not compacted
                assertThat(split.dataFiles().size()).isEqualTo(2);
            }
        }
    }

    @Test
    public void testStreamingCompact() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " k INT,"
                        + " v INT,"
                        + " hh INT,"
                        + " dt STRING,"
                        + " PRIMARY KEY (k, dt, hh) NOT ENFORCED"
                        + ") PARTITIONED BY (dt, hh) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'bucket' = '1',"
                        + " 'changelog-producer' = 'full-compaction',"
                        + " 'full-compaction.delta-commits' = '1',"
                        + " 'continuous.discovery-interval' = '1s'"
                        + ")");
        FileStoreTable table = paimonTable("T");
        BlockingIterator<Row, Row> select = streamSqlBlockIter("SELECT * FROM T");

        sql(
                "INSERT INTO T VALUES (1, 100, 15, '20221208'), (1, 100, 16, '20221208'), (1, 100, 15, '20221209')");
        checkLatestSnapshot(table, 1, Snapshot.CommitKind.APPEND);

        // no full compaction has happened, so plan should be empty
        StreamTableScan scan = table.newReadBuilder().newStreamScan();
        TableScan.Plan plan = scan.plan();
        assertThat(plan.splits()).isEmpty();

        ThreadLocalRandom random = ThreadLocalRandom.current();
        // submit streaming compaction job
        if (random.nextBoolean()) {
            streamSqlIter(
                            "CALL sys.compact(`table` => 'default.T', partitions => 'dt=20221208,hh=15;dt=20221209,hh=15', "
                                    + "options => 'scan.parallelism=1')")
                    .close();
        } else {
            streamSqlIter(
                            "CALL sys.compact(`table` => 'default.T', `where` => '(dt=20221208 and hh=15) or (dt=20221209 and hh=15)', "
                                    + "options => 'scan.parallelism=1')")
                    .close();
        }

        // first full compaction
        assertThat(select.collect(2))
                .containsExactlyInAnyOrder(
                        Row.of(1, 100, 15, "20221208"), Row.of(1, 100, 15, "20221209"));

        // incremental records
        sql(
                "INSERT INTO T VALUES (1, 101, 15, '20221208'), (1, 101, 16, '20221208'), (1, 101, 15, '20221209')");

        // second full compaction
        assertThat(select.collect(4))
                .containsExactlyInAnyOrder(
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, 100, 15, "20221208"),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, 101, 15, "20221208"),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, 100, 15, "20221209"),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, 101, 15, "20221209"));

        select.close();
    }

    @Test
    public void testHistoryPartitionCompact() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " k INT,"
                        + " v INT,"
                        + " hh INT,"
                        + " dt STRING,"
                        + " PRIMARY KEY (k, dt, hh) NOT ENFORCED"
                        + ") PARTITIONED BY (dt, hh) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'bucket' = '1'"
                        + ")");
        FileStoreTable table = paimonTable("T");

        sql(
                "INSERT INTO T VALUES (1, 100, 15, '20221208'), (1, 100, 16, '20221208'), (1, 100, 15, '20221209')");
        sql(
                "INSERT INTO T VALUES (2, 100, 15, '20221208'), (2, 100, 16, '20221208'), (2, 100, 15, '20221209')");

        Thread.sleep(5000);
        sql("INSERT INTO T VALUES (3, 100, 16, '20221208')");

        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);

        sql("CALL sys.compact(`table` => 'default.T', partition_idle_time => '5s')");

        checkLatestSnapshot(table, 4, Snapshot.CommitKind.COMPACT);

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        assertThat(splits.size()).isEqualTo(3);
        for (DataSplit split : splits) {
            if (split.partition().getInt(1) == 15) {
                // compacted
                assertThat(split.dataFiles().size()).isEqualTo(1);
            } else {
                // not compacted
                assertThat(split.dataFiles().size()).isEqualTo(3);
            }
        }
    }

    // ----------------------- Sort Compact -----------------------

    @Test
    public void testDynamicBucketSortCompact() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " f0 BIGINT PRIMARY KEY NOT ENFORCED,"
                        + " f1 BIGINT,"
                        + " f2 BIGINT,"
                        + " f3 BIGINT,"
                        + " f4 STRING"
                        + ") WITH ("
                        + " 'write-only' = 'true',"
                        + " 'dynamic-bucket.target-row-num' = '100',"
                        + " 'zorder.var-length-contribution' = '14'"
                        + ")");
        FileStoreTable table = paimonTable("T");

        ThreadLocalRandom random = ThreadLocalRandom.current();
        int commitTimes = 20;
        for (int i = 0; i < commitTimes; i++) {
            String value =
                    IntStream.range(0, 100)
                            .mapToObj(
                                    in ->
                                            String.format(
                                                    "(%s, %s, %s, %s, '%s')",
                                                    random.nextLong(),
                                                    random.nextLong(),
                                                    random.nextLong(),
                                                    random.nextLong(),
                                                    StringUtils.randomNumericString(14)))
                            .collect(Collectors.joining(","));

            sql("INSERT INTO T VALUES %s", value);
        }
        checkLatestSnapshot(table, 20, Snapshot.CommitKind.APPEND);

        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        sql(
                "CALL sys.compact(`table` => 'default.T', order_strategy => 'zorder', order_by => 'f2,f1')");

        checkLatestSnapshot(table, 21, Snapshot.CommitKind.OVERWRITE);
    }

    // ----------------------- Minor Compact -----------------------

    @Test
    public void testBatchMinorCompactStrategy() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " k INT,"
                        + " v INT,"
                        + " hh INT,"
                        + " dt STRING,"
                        + " PRIMARY KEY (k, dt, hh) NOT ENFORCED"
                        + ") PARTITIONED BY (dt, hh) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'bucket' = '1'"
                        + ")");
        FileStoreTable table = paimonTable("T");
        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);

        sql("INSERT INTO T VALUES (1, 100, 15, '20221208'), (1, 100, 16, '20221208')");
        sql("INSERT INTO T VALUES (2, 100, 15, '20221208'), (2, 100, 16, '20221208')");

        checkLatestSnapshot(table, 2, Snapshot.CommitKind.APPEND);

        sql(
                "CALL sys.compact(`table` => 'default.T', compact_strategy => 'minor', "
                        + "options => 'num-sorted-run.compaction-trigger=3')");

        // Due to the limitation of parameter 'num-sorted-run.compaction-trigger' = 3, so compact is
        // not
        // performed.
        checkLatestSnapshot(table, 2, Snapshot.CommitKind.APPEND);

        // Make par-15 has 3 datafile and par-16 has 2 datafile, so par-16 will not be picked out to
        // compact.
        sql("INSERT INTO T VALUES (1, 100, 15, '20221208')");

        sql(
                "CALL sys.compact(`table` => 'default.T', compact_strategy => 'minor', "
                        + "options => 'num-sorted-run.compaction-trigger=3')");

        checkLatestSnapshot(table, 4, Snapshot.CommitKind.COMPACT);

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        assertThat(splits.size()).isEqualTo(2);
        for (DataSplit split : splits) {
            // Par-16 is not compacted.
            assertThat(split.dataFiles().size())
                    .isEqualTo(split.partition().getInt(1) == 16 ? 2 : 1);
        }
    }

    @Test
    public void testBatchFullCompactStrategy() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " k INT,"
                        + " v INT,"
                        + " hh INT,"
                        + " dt STRING,"
                        + " PRIMARY KEY (k, dt, hh) NOT ENFORCED"
                        + ") PARTITIONED BY (dt, hh) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'bucket' = '1'"
                        + ")");
        FileStoreTable table = paimonTable("T");
        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);

        sql("INSERT INTO T VALUES (1, 100, 15, '20221208'), (1, 100, 16, '20221208')");
        sql("INSERT INTO T VALUES (2, 100, 15, '20221208'), (2, 100, 16, '20221208')");

        checkLatestSnapshot(table, 2, Snapshot.CommitKind.APPEND);

        sql(
                "CALL sys.compact(`table` => 'default.T', compact_strategy => 'full', "
                        + "options => 'num-sorted-run.compaction-trigger=3')");

        checkLatestSnapshot(table, 3, Snapshot.CommitKind.COMPACT);

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        assertThat(splits.size()).isEqualTo(2);
        for (DataSplit split : splits) {
            // Par-16 is not compacted.
            assertThat(split.dataFiles().size()).isEqualTo(1);
        }
    }

    @Test
    public void testStreamFullCompactStrategy() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " k INT,"
                        + " v INT,"
                        + " hh INT,"
                        + " dt STRING,"
                        + " PRIMARY KEY (k, dt, hh) NOT ENFORCED"
                        + ") PARTITIONED BY (dt, hh) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'bucket' = '1'"
                        + ")");
        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);

        Assertions.assertThatThrownBy(
                        () ->
                                streamSqlIter(
                                                "CALL sys.compact(`table` => 'default.T', compact_strategy => 'full', "
                                                        + "options => 'num-sorted-run.compaction-trigger=3')")
                                        .close())
                .hasMessageContaining(
                        "The full compact strategy is only supported in batch mode. Please add -Dexecution.runtime-mode=BATCH.");
    }

    private void checkLatestSnapshot(
            FileStoreTable table, long snapshotId, Snapshot.CommitKind commitKind) {
        SnapshotManager snapshotManager = table.snapshotManager();
        Snapshot snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());
        assertThat(snapshot.id()).isEqualTo(snapshotId);
        assertThat(snapshot.commitKind()).isEqualTo(commitKind);
    }
}
