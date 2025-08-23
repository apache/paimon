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

package org.apache.paimon.flink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSource;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSourceReader;
import org.apache.paimon.flink.source.SimpleSourceSplit;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.utils.FailingFileIO;
import org.apache.paimon.utils.TimeUtils;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.apache.paimon.CoreOptions.PARTITION_SINK_STRATEGY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test case for append table. */
public class AppendTableITCase extends CatalogITCaseBase {

    private static final Random RANDOM = new Random();

    @Test
    public void testReadEmpty() {
        assertThat(batchSql("SELECT * FROM append_table")).isEmpty();
    }

    @Test
    public void testReadWrite() {
        batchSql("INSERT INTO append_table VALUES (1, 'AAA'), (2, 'BBB')");

        List<Row> rows = batchSql("SELECT * FROM append_table");
        assertThat(rows.size()).isEqualTo(2);
        assertThat(rows).containsExactlyInAnyOrder(Row.of(1, "AAA"), Row.of(2, "BBB"));

        rows = batchSql("SELECT id FROM append_table");
        assertThat(rows.size()).isEqualTo(2);
        assertThat(rows).containsExactlyInAnyOrder(Row.of(1), Row.of(2));

        rows = batchSql("SELECT data from append_table");
        assertThat(rows.size()).isEqualTo(2);
        assertThat(rows).containsExactlyInAnyOrder(Row.of("AAA"), Row.of("BBB"));
    }

    @Test
    public void testReadWriteWithLineage() {
        batchSql("INSERT INTO append_table_lineage VALUES (1, 'AAA'), (2, 'BBB')");
        List<Row> rows = batchSql("SELECT * FROM append_table_lineage$row_lineage");
        assertThat(rows.size()).isEqualTo(2);
        assertThat(rows)
                .containsExactlyInAnyOrder(Row.of(1, "AAA", 0L, 1L), Row.of(2, "BBB", 1L, 1L));

        rows = batchSql("SELECT * FROM append_table_lineage");
        assertThat(rows.size()).isEqualTo(2);
        assertThat(rows).containsExactlyInAnyOrder(Row.of(1, "AAA"), Row.of(2, "BBB"));
    }

    @Test
    public void testCompactionWithRowLineage() throws Exception {
        batchSql("ALTER TABLE append_table_lineage SET ('compaction.max.file-num' = '4')");

        assertExecuteExpected(
                "INSERT INTO append_table_lineage VALUES (1, 'AAA'), (2, 'BBB')",
                1L,
                Snapshot.CommitKind.APPEND,
                "append_table_lineage");
        assertExecuteExpected(
                "INSERT INTO append_table_lineage VALUES (3, 'CCC'), (4, 'DDD')",
                2L,
                Snapshot.CommitKind.APPEND,
                "append_table_lineage");
        assertExecuteExpected(
                "INSERT INTO append_table_lineage VALUES (1, 'AAA'), (2, 'BBB'), (3, 'CCC'), (4, 'DDD')",
                3L,
                Snapshot.CommitKind.APPEND,
                "append_table_lineage");
        assertExecuteExpected(
                "INSERT INTO append_table_lineage VALUES (5, 'EEE'), (6, 'FFF')",
                4L,
                Snapshot.CommitKind.APPEND,
                "append_table_lineage");
        assertExecuteExpected(
                "INSERT INTO append_table_lineage VALUES (7, 'HHH'), (8, 'III')",
                5L,
                Snapshot.CommitKind.APPEND,
                "append_table_lineage");
        assertExecuteExpected(
                "INSERT INTO append_table_lineage VALUES (9, 'JJJ'), (10, 'KKK')",
                6L,
                Snapshot.CommitKind.APPEND,
                "append_table_lineage");
        assertExecuteExpected(
                "INSERT INTO append_table_lineage VALUES (11, 'LLL'), (12, 'MMM')",
                7L,
                Snapshot.CommitKind.APPEND,
                "append_table_lineage");
        assertExecuteExpected(
                "INSERT INTO append_table_lineage VALUES (13, 'NNN'), (14, 'OOO')",
                8L,
                Snapshot.CommitKind.APPEND,
                "append_table_lineage");

        List<Row> originRowsWithId2 = batchSql("SELECT * FROM append_table_lineage$row_lineage");
        batchSql("call sys.compact('default.append_table_lineage')");
        waitCompactSnapshot(60000L, "append_table_lineage");
        List<Row> files = batchSql("SELECT * FROM append_table_lineage$files");
        assertThat(files.size()).isEqualTo(1);
        List<Row> rowsAfter2 = batchSql("SELECT * FROM append_table_lineage$row_lineage");
        assertThat(originRowsWithId2).containsExactlyInAnyOrderElementsOf(rowsAfter2);

        assertThat(rowsAfter2)
                .containsExactlyInAnyOrder(
                        Row.of(1, "AAA", 0L, 1L),
                        Row.of(2, "BBB", 1L, 1L),
                        Row.of(3, "CCC", 2L, 2L),
                        Row.of(4, "DDD", 3L, 2L),
                        Row.of(1, "AAA", 4L, 3L),
                        Row.of(2, "BBB", 5L, 3L),
                        Row.of(3, "CCC", 6L, 3L),
                        Row.of(4, "DDD", 7L, 3L),
                        Row.of(5, "EEE", 8L, 4L),
                        Row.of(6, "FFF", 9L, 4L),
                        Row.of(7, "HHH", 10L, 5L),
                        Row.of(8, "III", 11L, 5L),
                        Row.of(9, "JJJ", 12L, 6L),
                        Row.of(10, "KKK", 13L, 6L),
                        Row.of(11, "LLL", 14L, 7L),
                        Row.of(12, "MMM", 15L, 7L),
                        Row.of(13, "NNN", 16L, 8L),
                        Row.of(14, "OOO", 17L, 8L));
    }

    @Test
    public void testSkipDedup() {
        batchSql("INSERT INTO append_table VALUES (1, 'AAA'), (1, 'AAA'), (2, 'BBB'), (3, 'BBB')");

        List<Row> rows = batchSql("SELECT * FROM append_table");
        assertThat(rows.size()).isEqualTo(4);
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of(1, "AAA"), Row.of(1, "AAA"), Row.of(2, "BBB"), Row.of(3, "BBB"));

        rows = batchSql("SELECT id FROM append_table");
        assertThat(rows.size()).isEqualTo(4);
        assertThat(rows).containsExactlyInAnyOrder(Row.of(1), Row.of(1), Row.of(2), Row.of(3));

        rows = batchSql("SELECT data FROM append_table");
        assertThat(rows.size()).isEqualTo(4);
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of("AAA"), Row.of("AAA"), Row.of("BBB"), Row.of("BBB"));
    }

    @Test
    public void testIngestFromSource() {
        List<Row> input =
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, 1, "AAA"),
                        Row.ofKind(RowKind.INSERT, 1, "AAA"),
                        Row.ofKind(RowKind.INSERT, 1, "BBB"),
                        Row.ofKind(RowKind.INSERT, 2, "AAA"));

        String id = TestValuesTableFactory.registerData(input);
        batchSql(
                "CREATE TEMPORARY TABLE source (id INT, data STRING) WITH ('connector'='values', 'bounded'='true', 'data-id'='%s')",
                id);

        batchSql("INSERT INTO append_table SELECT * FROM source");

        List<Row> rows = batchSql("SELECT * FROM append_table");
        assertThat(rows.size()).isEqualTo(4);
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of(1, "AAA"), Row.of(1, "AAA"), Row.of(1, "BBB"), Row.of(2, "AAA"));

        rows = batchSql("SELECT id FROM append_table");
        assertThat(rows.size()).isEqualTo(4);
        assertThat(rows).containsExactlyInAnyOrder(Row.of(1), Row.of(1), Row.of(1), Row.of(2));

        rows = batchSql("SELECT data FROM append_table");
        assertThat(rows.size()).isEqualTo(4);
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of("AAA"), Row.of("AAA"), Row.of("BBB"), Row.of("AAA"));
    }

    @Test
    public void testNoCompactionInBatchMode() {
        batchSql("ALTER TABLE append_table SET ('compaction.min.file-num' = '4')");

        assertExecuteExpected(
                "INSERT INTO append_table VALUES (1, 'AAA'), (2, 'BBB')",
                1L,
                Snapshot.CommitKind.APPEND);
        assertExecuteExpected(
                "INSERT INTO append_table VALUES (3, 'CCC'), (4, 'DDD')",
                2L,
                Snapshot.CommitKind.APPEND);
        assertExecuteExpected(
                "INSERT INTO append_table VALUES (1, 'AAA'), (2, 'BBB'), (3, 'CCC'), (4, 'DDD')",
                3L,
                Snapshot.CommitKind.APPEND);
        assertExecuteExpected(
                "INSERT INTO append_table VALUES (5, 'EEE'), (6, 'FFF')",
                4L,
                Snapshot.CommitKind.APPEND);
        assertExecuteExpected(
                "INSERT INTO append_table VALUES (7, 'HHH'), (8, 'III')",
                5L,
                Snapshot.CommitKind.APPEND);
        assertExecuteExpected(
                "INSERT INTO append_table VALUES (9, 'JJJ'), (10, 'KKK')",
                6L,
                Snapshot.CommitKind.APPEND);
        assertExecuteExpected(
                "INSERT INTO append_table VALUES (11, 'LLL'), (12, 'MMM')",
                7L,
                Snapshot.CommitKind.APPEND);
        assertExecuteExpected(
                "INSERT INTO append_table VALUES (13, 'NNN'), (14, 'OOO')",
                8L,
                Snapshot.CommitKind.APPEND);

        List<Row> rows = batchSql("SELECT * FROM append_table");
        assertThat(rows.size()).isEqualTo(18);
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of(1, "AAA"),
                        Row.of(2, "BBB"),
                        Row.of(3, "CCC"),
                        Row.of(4, "DDD"),
                        Row.of(1, "AAA"),
                        Row.of(2, "BBB"),
                        Row.of(3, "CCC"),
                        Row.of(4, "DDD"),
                        Row.of(5, "EEE"),
                        Row.of(6, "FFF"),
                        Row.of(7, "HHH"),
                        Row.of(8, "III"),
                        Row.of(9, "JJJ"),
                        Row.of(10, "KKK"),
                        Row.of(11, "LLL"),
                        Row.of(12, "MMM"),
                        Row.of(13, "NNN"),
                        Row.of(14, "OOO"));
    }

    @Test
    public void testCompactionInStreamingMode() throws Exception {
        batchSql("ALTER TABLE append_table SET ('compaction.min.file-num' = '4')");
        batchSql("ALTER TABLE append_table SET ('continuous.discovery-interval' = '1 s')");

        sEnv.getConfig()
                .getConfiguration()
                .setString(
                        "execution.checkpointing.interval",
                        TimeUtils.formatWithHighestUnit(Duration.ofMillis(500)));
        sEnv.executeSql(
                "CREATE TEMPORARY TABLE Orders_in (\n"
                        + "    f0        INT,\n"
                        + "    f1        STRING\n"
                        + ") WITH (\n"
                        + "    'connector' = 'datagen',\n"
                        + "    'rows-per-second' = '1'\n"
                        + ")");

        assertStreamingHasCompact("INSERT INTO append_table SELECT * FROM Orders_in", 60000);
        // ensure data gen finished
        Thread.sleep(5000);
    }

    @Test
    public void testCompactionInStreamingModeWithMaxWatermark() throws Exception {
        batchSql("ALTER TABLE append_table SET ('compaction.min.file-num' = '4')");
        batchSql("ALTER TABLE append_table SET ('continuous.discovery-interval' = '1 s')");

        sEnv.getConfig()
                .getConfiguration()
                .setString(
                        "execution.checkpointing.interval",
                        TimeUtils.formatWithHighestUnit(Duration.ofMillis(500)));
        sEnv.executeSql(
                "CREATE TEMPORARY TABLE Orders_in (\n"
                        + "    f0        INT,\n"
                        + "    f1        STRING,\n"
                        + "    ts        TIMESTAMP(3),\n"
                        + "WATERMARK FOR ts AS ts - INTERVAL '0' SECOND"
                        + ") WITH (\n"
                        + "    'connector' = 'datagen',\n"
                        + "    'rows-per-second' = '1',\n"
                        + "    'number-of-rows' = '10'\n"
                        + ")");

        assertStreamingHasCompact("INSERT INTO append_table SELECT f0, f1 FROM Orders_in", 60000);
        // ensure data gen finished
        Thread.sleep(5000);

        Snapshot snapshot = findLatestSnapshot("append_table");
        Assertions.assertNotNull(snapshot);
        Long watermark = snapshot.watermark();
        Assertions.assertNotNull(watermark);
        Assertions.assertTrue(watermark > Long.MIN_VALUE);
    }

    @Test
    public void testRejectDelete() {
        testRejectChanges(RowKind.DELETE);
    }

    @Test
    public void testRejectUpdateBefore() {
        testRejectChanges(RowKind.UPDATE_BEFORE);
    }

    @Test
    public void testRejectUpdateAfter() {
        testRejectChanges(RowKind.UPDATE_BEFORE);
    }

    @Test
    public void testComplexType() {
        batchSql("INSERT INTO complex_table VALUES (1, CAST(NULL AS MAP<INT, INT>))");
        assertThat(batchSql("SELECT * FROM complex_table")).containsExactly(Row.of(1, null));
    }

    @Test
    public void testTimestampLzType() {
        sql("CREATE TABLE t_table (id INT, data TIMESTAMP_LTZ(3))");
        batchSql("INSERT INTO t_table VALUES (1, TIMESTAMP '2023-02-03 20:20:20')");
        assertThat(batchSql("SELECT * FROM t_table"))
                .containsExactly(
                        Row.of(
                                1,
                                LocalDateTime.parse("2023-02-03T20:20:20")
                                        .atZone(ZoneId.systemDefault())
                                        .toInstant()));
    }

    // test is not correct, append table may insert twice if always retry when file io fails
    @Test
    public void testReadWriteFailRandom() throws Exception {
        setFailRate(100, 1000);
        int size = 1000;
        List<Row> results = new ArrayList<>();
        StringBuilder values = new StringBuilder("");
        for (int i = 0; i < size; i++) {
            Integer j = RANDOM.nextInt();
            results.add(Row.of(j, String.valueOf(j)));
            values.append("(" + j + ",'" + j + "'" + "),");
        }

        FailingFileIO.retryArtificialException(
                () ->
                        batchSql(
                                String.format(
                                        "INSERT INTO append_table VALUES %s",
                                        values.toString().substring(0, values.length() - 1))));

        FailingFileIO.retryArtificialException(
                () -> {
                    batchSql("SELECT * FROM append_table");
                    List<Row> rows = batchSql("SELECT * FROM append_table");
                    assertThat(rows.size()).isGreaterThanOrEqualTo(size);
                    assertThat(rows).containsExactlyInAnyOrder(results.toArray(new Row[0]));
                });
    }

    @Test
    public void testReadWriteFailRandomString() throws Exception {
        setFailRate(100, 1000);
        int size = 1000;
        List<Row> results = new ArrayList<>();
        StringBuilder values = new StringBuilder("");
        for (int i = 0; i < size; i++) {
            Integer j = RANDOM.nextInt();
            String v = String.valueOf(RANDOM.nextInt());
            results.add(Row.of(j, v));
            values.append("(" + j + ",'" + v + "'" + "),");
        }

        FailingFileIO.retryArtificialException(
                () ->
                        batchSql(
                                String.format(
                                        "INSERT INTO append_table VALUES %s",
                                        values.toString().substring(0, values.length() - 1))));

        FailingFileIO.retryArtificialException(
                () -> {
                    batchSql("SELECT * FROM append_table");
                    List<Row> rows = batchSql("SELECT * FROM append_table");
                    assertThat(rows.size()).isGreaterThanOrEqualTo(size);
                    assertThat(rows).containsExactlyInAnyOrder(results.toArray(new Row[0]));
                });
    }

    @Test
    public void testLimit() {
        sql("INSERT INTO append_table VALUES (1, 'AAA')");
        sql("INSERT INTO append_table VALUES (2, 'BBB')");
        assertThat(sql("SELECT * FROM append_table LIMIT 1")).hasSize(1);
    }

    @Test
    public void testFileIndex() {
        batchSql(
                "INSERT INTO index_table VALUES (1, 'a', 'AAA'), (1, 'a', 'AAA'), (2, 'c', 'BBB'), (3, 'c', 'BBB')");
        batchSql(
                "INSERT INTO index_table VALUES (1, 'a', 'AAA'), (1, 'a', 'AAA'), (2, 'd', 'BBB'), (3, 'd', 'BBB')");

        assertThat(batchSql("SELECT * FROM index_table WHERE indexc = 'c' and (id = 2 or id = 3)"))
                .containsExactlyInAnyOrder(Row.of(2, "c", "BBB"), Row.of(3, "c", "BBB"));
    }

    @Timeout(60)
    @Test
    public void testStatelessWriter() throws Exception {
        FileStoreTable table =
                FileStoreTableFactory.create(
                        LocalFileIO.create(), new Path(path, "default.db/append_table"));

        StreamExecutionEnvironment env =
                streamExecutionEnvironmentBuilder()
                        .streamingMode()
                        .parallelism(2)
                        .checkpointIntervalMs(500)
                        .build();
        DataStream<Integer> source =
                env.fromSource(
                                new TestStatelessWriterSource(table),
                                WatermarkStrategy.noWatermarks(),
                                "TestStatelessWriterSource")
                        .setParallelism(2)
                        .forward();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.registerCatalog("mycat", sEnv.getCatalog("PAIMON").get());
        tEnv.executeSql("USE CATALOG mycat");
        tEnv.createTemporaryView("S", tEnv.fromDataStream(source).as("id"));

        tEnv.executeSql("INSERT INTO append_table SELECT id, 'test' FROM S").await();
        assertThat(batchSql("SELECT * FROM append_table"))
                .containsExactlyInAnyOrder(Row.of(1, "test"), Row.of(2, "test"));
    }

    @ParameterizedTest
    @EnumSource(CoreOptions.PartitionSinkStrategy.class)
    public void testPartitionStrategyForPartitionedTable(CoreOptions.PartitionSinkStrategy strategy)
            throws Catalog.TableNotExistException {

        int partitionNums = 5;
        int largerSinkParallelism = 7;
        int lessSinkParallelism = 3;
        int hashStrategyResultFileCount = 1;
        // sink parallelism is greater than the number of partitions write in a batch, there are 2
        // task will be no data.
        batchSql(
                "CREATE TABLE IF NOT EXISTS partition_strategy_table_larger ("
                        + "id INT, data STRING, dt STRING) PARTITIONED BY (dt)"
                        + " WITH ("
                        + "'bucket' = '-1',"
                        + "'%s' = '%s',"
                        + "'sink.parallelism' = '7')",
                PARTITION_SINK_STRATEGY.key(), strategy);

        // sink parallelism is less than the number of partitions write in a batch, there are 2 task
        // will write data to 2 partition.
        batchSql(
                "CREATE TABLE IF NOT EXISTS partition_strategy_table_less ("
                        + "id INT, data STRING, dt STRING) PARTITIONED BY (dt)"
                        + " WITH ("
                        + "'bucket' = '-1',"
                        + "'%s' = '%s',"
                        + "'sink.parallelism' = '3')",
                PARTITION_SINK_STRATEGY.key(), strategy);

        StringBuilder values = new StringBuilder();
        // 5 partition in a batch write.
        for (int i = 1; i <= 30; i++) {
            for (int j = 1; j <= partitionNums; j++) {
                values.append(String.format("(%s, 'HXH', '2025030%s'),", j, j));
            }
        }

        batchSql(
                "INSERT INTO partition_strategy_table_larger VALUES "
                        + values.substring(0, values.length() - 1));
        batchSql(
                "INSERT INTO partition_strategy_table_less VALUES "
                        + values.substring(0, values.length() - 1));

        assertThat(batchSql("SELECT * FROM partition_strategy_table_larger").size()).isEqualTo(150);
        assertThat(batchSql("SELECT * FROM partition_strategy_table_less").size()).isEqualTo(150);

        FileStoreTable fileStoreTableLarger = paimonTable("partition_strategy_table_larger");
        List<PartitionEntry> partitionEntriesLarger =
                fileStoreTableLarger.newReadBuilder().newScan().listPartitionEntries();
        assertThat(partitionEntriesLarger.size()).isEqualTo(partitionNums);
        int fileCountLarger =
                strategy == CoreOptions.PartitionSinkStrategy.HASH
                        ? hashStrategyResultFileCount
                        : largerSinkParallelism;
        partitionEntriesLarger.forEach(x -> assertThat(x.fileCount()).isEqualTo(fileCountLarger));

        FileStoreTable fileStoreTableLess = paimonTable("partition_strategy_table_less");
        List<PartitionEntry> partitionEntriesLess =
                fileStoreTableLess.newReadBuilder().newScan().listPartitionEntries();
        assertThat(partitionEntriesLess.size()).isEqualTo(partitionNums);
        int fileCountLess =
                strategy == CoreOptions.PartitionSinkStrategy.HASH
                        ? hashStrategyResultFileCount
                        : lessSinkParallelism;
        partitionEntriesLess.forEach(x -> assertThat(x.fileCount()).isEqualTo(fileCountLess));
    }

    @Test
    public void testFlinkMemoryPool() throws Exception {
        batchSql("ALTER TABLE append_table SET ('sink.use-managed-memory-allocator' = 'true')");
        sEnv.executeSql(
                "CREATE TEMPORARY TABLE Orders_in (\n"
                        + "    f0        INT,\n"
                        + "    f1        STRING\n"
                        + ") WITH (\n"
                        + "    'connector' = 'datagen',\n"
                        + "    'number-of-rows' = '10'\n"
                        + ")");

        sEnv.executeSql("INSERT INTO append_table SELECT * FROM Orders_in").await();
        assertThat(batchSql("SELECT * FROM append_table").size()).isEqualTo(10);
    }

    private static class TestStatelessWriterSource extends AbstractNonCoordinatedSource<Integer> {

        private final FileStoreTable table;

        private TestStatelessWriterSource(FileStoreTable table) {
            this.table = table;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        }

        @Override
        public SourceReader<Integer, SimpleSourceSplit> createReader(
                SourceReaderContext sourceReaderContext) throws Exception {
            return new Reader(sourceReaderContext.getIndexOfSubtask());
        }

        private class Reader extends AbstractNonCoordinatedSourceReader<Integer> {
            private final int taskId;
            private int waitCount;

            private Reader(int taskId) {
                this.taskId = taskId;
                this.waitCount = (taskId == 0 ? 0 : 10);
            }

            @Override
            public InputStatus pollNext(ReaderOutput<Integer> readerOutput) throws Exception {
                if (taskId == 0) {
                    if (waitCount == 0) {
                        readerOutput.collect(1);
                    } else if (countNumRecords() >= 1) {
                        // wait for the record to commit before exiting
                        Thread.sleep(1000);
                        return InputStatus.END_OF_INPUT;
                    }
                } else {
                    int numRecords = countNumRecords();
                    if (numRecords >= 1) {
                        if (waitCount == 0) {
                            readerOutput.collect(2);
                        } else if (countNumRecords() >= 2) {
                            // make sure the next checkpoint is successful
                            Thread.sleep(1000);
                            return InputStatus.END_OF_INPUT;
                        }
                    }
                }
                waitCount--;
                Thread.sleep(1000);
                return InputStatus.MORE_AVAILABLE;
            }
        }

        private int countNumRecords() throws Exception {
            int ret = 0;
            RecordReader<InternalRow> reader =
                    table.newRead().createReader(table.newSnapshotReader().read());
            try (RecordReaderIterator<InternalRow> it = new RecordReaderIterator<>(reader)) {
                while (it.hasNext()) {
                    it.next();
                    ret++;
                }
            }
            return ret;
        }
    }

    @Override
    protected List<String> ddl() {
        return Arrays.asList(
                "CREATE TABLE IF NOT EXISTS append_table (id INT, data STRING) WITH ('bucket' = '-1')",
                "CREATE TABLE IF NOT EXISTS append_table_lineage (id INT, data STRING) WITH ('bucket' = '-1', 'row-tracking.enabled' = 'true')",
                "CREATE TABLE IF NOT EXISTS part_table (id INT, data STRING, dt STRING) PARTITIONED BY (dt) WITH ('bucket' = '-1')",
                "CREATE TABLE IF NOT EXISTS complex_table (id INT, data MAP<INT, INT>) WITH ('bucket' = '-1')",
                "CREATE TABLE IF NOT EXISTS index_table (id INT, indexc STRING, data STRING) WITH ('bucket' = '-1', 'file-index.bloom-filter.columns'='indexc', 'file-index.bloom-filter.indexc.items' = '500')");
    }

    @Override
    protected String toWarehouse(String path) {
        File file = new File(path);
        String dirName = file.getName();
        String dirPath = file.getPath();
        FailingFileIO.reset(dirName, 0, 1);
        return FailingFileIO.getFailingPath(dirName, dirPath);
    }

    private void setFailRate(int maxFails, int failPossibility) {
        FailingFileIO.reset(new Path(path).getName(), maxFails, failPossibility);
    }

    private void testRejectChanges(RowKind kind) {
        List<Row> input = Collections.singletonList(Row.ofKind(kind, 1, "AAA"));

        String id = TestValuesTableFactory.registerData(input);
        batchSql(
                "CREATE TEMPORARY TABLE source (id INT, data STRING) WITH ('connector'='values', 'bounded'='true', 'data-id'='%s')",
                id);

        assertThatThrownBy(() -> batchSql("INSERT INTO append_table SELECT * FROM source"))
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("Append only writer can not accept row with RowKind %s", kind);
    }

    private void assertExecuteExpected(
            String sql, long expectedSnapshotId, Snapshot.CommitKind expectedCommitKind) {
        assertExecuteExpected(sql, expectedSnapshotId, expectedCommitKind, "append_table");
    }

    private void assertExecuteExpected(
            String sql,
            long expectedSnapshotId,
            Snapshot.CommitKind expectedCommitKind,
            String tableName) {
        batchSql(sql);
        Snapshot snapshot = findLatestSnapshot(tableName);
        assertThat(snapshot.id()).isEqualTo(expectedSnapshotId);
        assertThat(snapshot.commitKind()).isEqualTo(expectedCommitKind);
    }

    private void assertBatchHasCompact(String sql, long timeout) throws Exception {
        batchSql(sql);
        waitCompactSnapshot(timeout);
    }

    private void assertStreamingHasCompact(String sql, long timeout) throws Exception {
        sEnv.executeSql(sql);
        waitCompactSnapshot(timeout);
    }

    private void waitCompactSnapshot(long timeout) throws Exception {
        waitCompactSnapshot(timeout, "append_table");
    }

    private void waitCompactSnapshot(long timeout, String tableName) throws Exception {
        long start = System.currentTimeMillis();
        long currentId = 1;
        Snapshot snapshot;
        while (true) {
            snapshot = findSnapshot(tableName, currentId);
            if (snapshot != null) {
                if (snapshot.commitKind() == Snapshot.CommitKind.COMPACT) {
                    break;
                }
                currentId++;
            }
            long now = System.currentTimeMillis();
            if (now - start > timeout) {
                throw new RuntimeException(
                        "Time up for streaming execute, don't get expected result.");
            }
            Thread.sleep(1000);
        }
    }
}
