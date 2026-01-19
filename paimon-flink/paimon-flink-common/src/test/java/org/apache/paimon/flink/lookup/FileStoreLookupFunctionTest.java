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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.lookup.PrimaryKeyPartialLookupTable.LocalQueryExecutor;
import org.apache.paimon.flink.lookup.PrimaryKeyPartialLookupTable.QueryExecutor;
import org.apache.paimon.flink.lookup.PrimaryKeyPartialLookupTable.RemoteQueryExecutor;
import org.apache.paimon.lookup.rocksdb.RocksDBOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.service.ServiceManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.TraceableFileIO;

import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_REFRESH_TIME_PERIODS_BLACKLIST;
import static org.apache.paimon.service.ServiceManager.PRIMARY_KEY_LOOKUP;
import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Tests for {@link FileStoreLookupFunction}. */
public class FileStoreLookupFunctionTest {

    private static final Random RANDOM = new Random();

    @TempDir private Path tempDir;

    private final String commitUser = UUID.randomUUID().toString();
    private final TraceableFileIO fileIO = new TraceableFileIO();

    private org.apache.paimon.fs.Path tablePath;
    private FileStoreLookupFunction lookupFunction;
    private FileStoreTable table;

    @BeforeEach
    public void before() throws Exception {
        tablePath = new org.apache.paimon.fs.Path(tempDir.toString());
    }

    private void createLookupFunction(boolean refreshAsync) throws Exception {
        createLookupFunction(true, false, false, refreshAsync, null);
    }

    private void createLookupFunction(
            boolean isPartition,
            boolean joinEqualPk,
            boolean dynamicPartition,
            boolean refreshAsync,
            Integer fullLoadThreshold)
            throws Exception {
        table =
                createFileStoreTable(
                        isPartition, dynamicPartition, refreshAsync, fullLoadThreshold);
        lookupFunction = createLookupFunction(table, joinEqualPk);
        lookupFunction.open(tempDir.toString());
    }

    private FileStoreLookupFunction createLookupFunction(
            FileStoreTable table, boolean joinEqualPk) {
        return new FileStoreLookupFunction(
                table,
                new int[] {0, 1},
                joinEqualPk ? new int[] {0, 1} : new int[] {1},
                null,
                null);
    }

    private FileStoreTable createFileStoreTable(
            boolean isPartition,
            boolean dynamicPartition,
            boolean refreshAsync,
            Integer fullLoadThreshold)
            throws Exception {
        SchemaManager schemaManager = new SchemaManager(fileIO, tablePath);
        Options conf = new Options();
        conf.set(FlinkConnectorOptions.LOOKUP_REFRESH_ASYNC, refreshAsync);
        conf.set(CoreOptions.BUCKET, 2);
        conf.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX, 3);
        conf.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN, 2);
        conf.set(RocksDBOptions.LOOKUP_CONTINUOUS_DISCOVERY_INTERVAL, Duration.ofSeconds(1));
        if (dynamicPartition) {
            conf.set(FlinkConnectorOptions.SCAN_PARTITIONS, "max_pt()");
        }

        if (fullLoadThreshold != null) {
            conf.set(FlinkConnectorOptions.LOOKUP_REFRESH_FULL_LOAD_THRESHOLD, fullLoadThreshold);
        }

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT()},
                        new String[] {"pt", "k", "v"});
        Schema schema =
                new Schema(
                        rowType.getFields(),
                        isPartition ? Collections.singletonList("pt") : Collections.emptyList(),
                        Arrays.asList("pt", "k"),
                        conf.toMap(),
                        "");
        TableSchema tableSchema = schemaManager.createTable(schema);
        return FileStoreTableFactory.create(
                fileIO, new org.apache.paimon.fs.Path(tempDir.toString()), tableSchema);
    }

    @AfterEach
    public void close() throws Exception {
        if (lookupFunction != null) {
            lookupFunction.close();
        }
    }

    @Test
    public void testCompatibilityForOldVersion() throws Exception {
        createLookupFunction(false, true, false, false, null);
        commit(writeCommit(1));
        PrimaryKeyPartialLookupTable lookupTable =
                (PrimaryKeyPartialLookupTable) lookupFunction.lookupTable();
        LocalQueryExecutor queryExecutor = (LocalQueryExecutor) lookupTable.queryExecutor();

        // set totalBuckets to null, for testing old version
        DataSplit split = (DataSplit) table.newReadBuilder().newScan().plan().splits().get(0);
        Field field = DataSplit.class.getDeclaredField("totalBuckets");
        field.setAccessible(true);
        field.set(split, null);
        assertThat(split.totalBuckets()).isNull();

        // assert num buckets should be 2
        queryExecutor.refreshSplit(split);
        assertThat(queryExecutor.numBuckets(EMPTY_ROW)).isEqualTo(2);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testDefaultLocalPartial(boolean refreshAsync) throws Exception {
        createLookupFunction(false, true, false, refreshAsync, null);
        assertThat(lookupFunction.lookupTable()).isInstanceOf(PrimaryKeyPartialLookupTable.class);
        QueryExecutor queryExecutor =
                ((PrimaryKeyPartialLookupTable) lookupFunction.lookupTable()).queryExecutor();
        assertThat(queryExecutor).isInstanceOf(LocalQueryExecutor.class);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testDefaultRemotePartial(boolean refreshAsync) throws Exception {
        createLookupFunction(false, true, false, refreshAsync, null);
        ServiceManager serviceManager = new ServiceManager(fileIO, tablePath);
        serviceManager.resetService(
                PRIMARY_KEY_LOOKUP, new InetSocketAddress[] {new InetSocketAddress(1)});
        lookupFunction.open(tempDir.toString());
        assertThat(lookupFunction.lookupTable()).isInstanceOf(PrimaryKeyPartialLookupTable.class);
        QueryExecutor queryExecutor =
                ((PrimaryKeyPartialLookupTable) lookupFunction.lookupTable()).queryExecutor();
        assertThat(queryExecutor).isInstanceOf(RemoteQueryExecutor.class);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testLookupScanLeak(boolean refreshAsync) throws Exception {
        createLookupFunction(refreshAsync);
        commit(writeCommit(1));
        lookupFunction.lookup(new FlinkRowData(GenericRow.of(1, 1, 10L)));
        assertThat(
                        TraceableFileIO.openInputStreams(
                                        s -> s.toString().contains(tempDir.toString()))
                                .size())
                .isEqualTo(0);

        commit(writeCommit(10));
        lookupFunction.lookup(new FlinkRowData(GenericRow.of(1, 1, 10L)));
        assertThat(
                        TraceableFileIO.openInputStreams(
                                        s -> s.toString().contains(tempDir.toString()))
                                .size())
                .isEqualTo(0);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testLookupExpiredSnapshot(boolean refreshAsync) throws Exception {
        createLookupFunction(refreshAsync);
        commit(writeCommit(1));
        lookupFunction.lookup(new FlinkRowData(GenericRow.of(1, 1, 10L)));

        commit(writeCommit(2));
        commit(writeCommit(3));
        commit(writeCommit(4));
        commit(writeCommit(5));
        lookupFunction.lookup(new FlinkRowData(GenericRow.of(1, 1, 10L)));
    }

    @Test
    public void testLookupDynamicPartition() throws Exception {
        createLookupFunction(true, false, true, false, null);
        commit(writeCommit(1));
        lookupFunction.lookup(new FlinkRowData(GenericRow.of(1, 1, 10L)));
        assertThat(
                        TraceableFileIO.openInputStreams(
                                        s -> s.toString().contains(tempDir.toString()))
                                .size())
                .isEqualTo(0);

        commit(writeCommit(10));
        lookupFunction.lookup(new FlinkRowData(GenericRow.of(1, 1, 10L)));
        assertThat(
                        TraceableFileIO.openInputStreams(
                                        s -> s.toString().contains(tempDir.toString()))
                                .size())
                .isEqualTo(0);
    }

    @Test
    public void testParseWrongTimePeriodsBlacklist() throws Exception {
        FileStoreTable table = createFileStoreTable(false, false, false, null);

        FileStoreTable table1 =
                table.copy(
                        Collections.singletonMap(
                                LOOKUP_REFRESH_TIME_PERIODS_BLACKLIST.key(),
                                "2024-10-31 12:00,2024-10-31 16:00"));
        assertThatThrownBy(() -> createLookupFunction(table1, true))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Incorrect time periods format: [2024-10-31 12:00,2024-10-31 16:00]."));

        FileStoreTable table2 =
                table.copy(
                        Collections.singletonMap(
                                LOOKUP_REFRESH_TIME_PERIODS_BLACKLIST.key(),
                                "20241031 12:00->20241031 16:00"));
        assertThatThrownBy(() -> createLookupFunction(table2, true))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Date time format error: [20241031 12:00]"));

        FileStoreTable table3 =
                table.copy(
                        Collections.singletonMap(
                                LOOKUP_REFRESH_TIME_PERIODS_BLACKLIST.key(),
                                "2024-10-31 12:00->2024-10-31 16:00,2024-10-31 20:00->2024-10-31 18:00"));
        assertThatThrownBy(() -> createLookupFunction(table3, true))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Incorrect time period: [2024-10-31 20:00->2024-10-31 18:00]"));
    }

    @Test
    public void testCheckRefreshInBlacklist() throws Exception {
        Instant now = Instant.now();
        Instant start = Instant.ofEpochSecond(now.getEpochSecond() / 60 * 60);
        Instant end = start.plusSeconds(30 * 60);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
        String left = start.atZone(ZoneId.systemDefault()).format(formatter);
        String right = end.atZone(ZoneId.systemDefault()).format(formatter);

        FileStoreTable table =
                createFileStoreTable(false, false, false, null)
                        .copy(
                                Collections.singletonMap(
                                        LOOKUP_REFRESH_TIME_PERIODS_BLACKLIST.key(),
                                        left + "->" + right));

        FileStoreLookupFunction lookupFunction = createLookupFunction(table, true);

        lookupFunction.tryRefresh();

        assertThat(lookupFunction.nextBlacklistCheckTime()).isEqualTo(end.toEpochMilli() + 1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testLookupTableWithFullLoad(boolean joinEqualPk) throws Exception {
        createLookupFunction(false, joinEqualPk, false, false, 3);

        if (joinEqualPk) {
            assertThat(lookupFunction.lookupTable())
                    .isInstanceOf(PrimaryKeyPartialLookupTable.class);
        } else {
            assertThat(lookupFunction.lookupTable()).isInstanceOf(FullCacheLookupTable.class);
        }

        GenericRow expectedRow = GenericRow.of(1, 1, 1L);
        StreamTableWrite writer = table.newStreamWriteBuilder().newWrite();
        writer.write(expectedRow);
        commit(writer.prepareCommit(true, 1));

        List<RowData> result =
                new ArrayList<>(lookupFunction.lookup(new FlinkRowData(GenericRow.of(1, 1, 1L))));
        assertThat(result).size().isEqualTo(1);
        RowData resultRow = result.get(0);
        assertThat(resultRow.getInt(0)).isEqualTo(expectedRow.getInt(0));
        assertThat(resultRow.getInt(1)).isEqualTo(expectedRow.getInt(1));

        // Create more commits to exceed threshold (3 more to have gap > 3)
        for (int i = 2; i < 6; i++) {
            writer.write(GenericRow.of(i, i, (long) i));
            commit(writer.prepareCommit(true, i));
        }
        writer.close();

        // wait refresh
        Thread.sleep(2000);

        expectedRow = GenericRow.of(5, 5, 5L);
        assertThat(lookupFunction.shouldDoFullLoad()).isTrue();
        lookupFunction.tryRefresh();
        result = new ArrayList<>(lookupFunction.lookup(new FlinkRowData(GenericRow.of(5, 5, 5L))));
        assertThat(result).size().isEqualTo(1);
        resultRow = result.get(0);
        assertThat(resultRow.getInt(0)).isEqualTo(expectedRow.getInt(0));
        assertThat(resultRow.getInt(1)).isEqualTo(expectedRow.getInt(1));
    }

    private void commit(List<CommitMessage> messages) throws Exception {
        TableCommitImpl commit = table.newCommit(commitUser);
        commit.commit(messages);
        commit.close();
    }

    private List<CommitMessage> writeCommit(int number) throws Exception {
        List<CommitMessage> messages = new ArrayList<>();
        StreamTableWrite writer = table.newStreamWriteBuilder().newWrite();
        for (int i = 0; i < number; i++) {
            writer.write(randomRow());
            messages.addAll(writer.prepareCommit(true, i));
        }
        return messages;
    }

    private InternalRow randomRow() {
        return GenericRow.of(RANDOM.nextInt(100), RANDOM.nextInt(100), RANDOM.nextLong());
    }
}
