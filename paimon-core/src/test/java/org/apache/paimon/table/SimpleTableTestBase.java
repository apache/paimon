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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.TestFileStore;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.operation.FileStoreTestUtils;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.ReaderSupplier;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.InnerTableCommit;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.OutOfRangeException;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;
import org.apache.paimon.utils.TraceableFileIO;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.BUCKET_KEY;
import static org.apache.paimon.CoreOptions.CHANGELOG_NUM_RETAINED_MAX;
import static org.apache.paimon.CoreOptions.CHANGELOG_NUM_RETAINED_MIN;
import static org.apache.paimon.CoreOptions.CONSUMER_IGNORE_PROGRESS;
import static org.apache.paimon.CoreOptions.DELETION_VECTORS_ENABLED;
import static org.apache.paimon.CoreOptions.ExpireExecutionMode;
import static org.apache.paimon.CoreOptions.FILE_FORMAT;
import static org.apache.paimon.CoreOptions.SNAPSHOT_CLEAN_EMPTY_DIRECTORIES;
import static org.apache.paimon.CoreOptions.SNAPSHOT_EXPIRE_EXECUTION_MODE;
import static org.apache.paimon.CoreOptions.SNAPSHOT_EXPIRE_LIMIT;
import static org.apache.paimon.CoreOptions.SNAPSHOT_NUM_RETAINED_MAX;
import static org.apache.paimon.CoreOptions.SNAPSHOT_NUM_RETAINED_MIN;
import static org.apache.paimon.CoreOptions.WRITE_ONLY;
import static org.apache.paimon.SnapshotTest.newSnapshotManager;
import static org.apache.paimon.format.FileFormat.fileFormat;
import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.apache.paimon.utils.HintFileUtils.EARLIEST;
import static org.apache.paimon.utils.HintFileUtils.LATEST;
import static org.apache.paimon.utils.NextSnapshotFetcher.RANGE_CHECK_INTERVAL;
import static org.apache.paimon.utils.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Base test class for simple table, simple representation means a fixed schema, see {@link
 * #ROW_TYPE}.
 */
public abstract class SimpleTableTestBase {

    protected static final String BRANCH_NAME = "branch1";

    protected static final RowType ROW_TYPE =
            RowType.of(
                    new DataType[] {
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.BIGINT(),
                        DataTypes.BINARY(1),
                        DataTypes.VARBINARY(1),
                        DataTypes.MAP(DataTypes.VARCHAR(8), DataTypes.VARCHAR(8)),
                        DataTypes.MULTISET(DataTypes.VARCHAR(8))
                    },
                    new String[] {"pt", "a", "b", "c", "d", "e", "f"});

    protected static final int[] PROJECTION = new int[] {2, 1};
    protected static final Function<InternalRow, String> BATCH_ROW_TO_STRING =
            rowData ->
                    rowData.getInt(0)
                            + "|"
                            + rowData.getInt(1)
                            + "|"
                            + rowData.getLong(2)
                            + "|"
                            + new String(rowData.getBinary(3))
                            + "|"
                            + new String(rowData.getBinary(4))
                            + "|"
                            + String.format(
                                    "%s:%s",
                                    rowData.getMap(5).keyArray().getString(0).toString(),
                                    rowData.getMap(5).valueArray().getString(0))
                            + "|"
                            + rowData.getMap(6).keyArray().getString(0).toString();
    protected static final Function<InternalRow, String> BATCH_PROJECTED_ROW_TO_STRING =
            rowData -> rowData.getLong(0) + "|" + rowData.getInt(1);
    protected static final Function<InternalRow, String> STREAMING_ROW_TO_STRING =
            rowData ->
                    (rowData.getRowKind() == RowKind.INSERT ? "+" : "-")
                            + BATCH_ROW_TO_STRING.apply(rowData);
    protected static final Function<InternalRow, String> STREAMING_PROJECTED_ROW_TO_STRING =
            rowData ->
                    (rowData.getRowKind() == RowKind.INSERT ? "+" : "-")
                            + BATCH_PROJECTED_ROW_TO_STRING.apply(rowData);
    protected static final Function<InternalRow, String> CHANGELOG_ROW_TO_STRING =
            rowData ->
                    rowData.getRowKind().shortString() + " " + BATCH_ROW_TO_STRING.apply(rowData);

    @TempDir java.nio.file.Path tempDir;

    protected Path tablePath;
    protected Identifier identifier;
    protected String commitUser;

    @BeforeEach
    public void before() throws Exception {
        identifier = Identifier.create("default", "table_test");
        tablePath = new Path(String.format("%s://%s", TraceableFileIO.SCHEME, tempDir.toString()));
        commitUser = UUID.randomUUID().toString();
    }

    @AfterEach
    public void after() throws Exception {
        // assert all connections are closed
        Predicate<Path> pathPredicate = path -> path.toString().contains(tempDir.toString());
        assertThat(TraceableFileIO.openInputStreams(pathPredicate)).isEmpty();
        assertThat(TraceableFileIO.openOutputStreams(pathPredicate)).isEmpty();
    }

    @Test
    public void testChangeFormat() throws Exception {
        FileStoreTable table =
                createFileStoreTable(conf -> conf.set(FILE_FORMAT, CoreOptions.FILE_FORMAT_ORC));

        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);
        write.write(rowData(1, 10, 100L));
        write.write(rowData(2, 20, 200L));
        commit.commit(0, write.prepareCommit(true, 0));
        write.close();
        commit.close();

        assertThat(
                        getResult(
                                table.newRead(),
                                toSplits(table.newSnapshotReader().read().dataSplits()),
                                BATCH_ROW_TO_STRING))
                .containsExactlyInAnyOrder(
                        "1|10|100|binary|varbinary|mapKey:mapVal|multiset",
                        "2|20|200|binary|varbinary|mapKey:mapVal|multiset");

        table =
                createFileStoreTable(
                        conf -> conf.set(FILE_FORMAT, CoreOptions.FILE_FORMAT_PARQUET));
        write = table.newWrite(commitUser);
        commit = table.newCommit(commitUser);
        write.write(rowData(1, 11, 111L));
        write.write(rowData(2, 22, 222L));
        commit.commit(1, write.prepareCommit(true, 1));
        write.close();
        commit.close();

        assertThat(
                        getResult(
                                table.newRead(),
                                toSplits(table.newSnapshotReader().read().dataSplits()),
                                BATCH_ROW_TO_STRING))
                .containsExactlyInAnyOrder(
                        "1|10|100|binary|varbinary|mapKey:mapVal|multiset",
                        "2|20|200|binary|varbinary|mapKey:mapVal|multiset",
                        "1|11|111|binary|varbinary|mapKey:mapVal|multiset",
                        "2|22|222|binary|varbinary|mapKey:mapVal|multiset");
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"avro", "orc", "parquet"})
    public void testMultipleCommits(String format) throws Exception {
        FileStoreTable table =
                createFileStoreTable(conf -> conf.setString(FILE_FORMAT.key(), format));
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 60; i++) {
            write.write(rowData(1, i, (long) i * 100));
            commit.commit(i, write.prepareCommit(true, i));
            expected.add(
                    String.format("1|%s|%s|binary|varbinary|mapKey:mapVal|multiset", i, i * 100));
        }

        write.close();
        commit.close();

        assertThat(
                        getResult(
                                table.newRead(),
                                toSplits(table.newSnapshotReader().read().dataSplits()),
                                BATCH_ROW_TO_STRING))
                .containsExactlyElementsOf(expected);
    }

    @Test
    public void testOverwrite() throws Exception {
        FileStoreTable table = createFileStoreTable();

        StreamTableWrite write = table.newWrite(commitUser);
        InnerTableCommit commit = table.newCommit(commitUser);
        write.write(rowData(1, 10, 100L));
        write.write(rowData(2, 20, 200L));
        commit.commit(0, write.prepareCommit(true, 0));
        write.close();
        commit.close();

        write = table.newWrite(commitUser).withIgnorePreviousFiles(true);
        commit = table.newCommit(commitUser);
        write.write(rowData(2, 21, 201L));
        Map<String, String> overwritePartition = new HashMap<>();
        overwritePartition.put("pt", "2");
        commit.withOverwrite(overwritePartition).commit(1, write.prepareCommit(true, 1));
        write.close();
        commit.close();

        List<Split> splits = toSplits(table.newSnapshotReader().read().dataSplits());
        TableRead read = table.newRead();
        assertThat(getResult(read, splits, binaryRow(1), 0, BATCH_ROW_TO_STRING))
                .hasSameElementsAs(
                        singletonList("1|10|100|binary|varbinary|mapKey:mapVal|multiset"));
        assertThat(getResult(read, splits, binaryRow(2), 0, BATCH_ROW_TO_STRING))
                .hasSameElementsAs(
                        singletonList("2|21|201|binary|varbinary|mapKey:mapVal|multiset"));
    }

    @Test
    public void testBucketFilter() throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        conf -> {
                            conf.set(BUCKET, 5);
                            conf.set(BUCKET_KEY, "a");
                        });

        StreamTableWrite write = table.newWrite(commitUser);
        write.write(rowData(1, 1, 2L));
        write.write(rowData(1, 3, 4L));
        write.write(rowData(1, 5, 6L));
        write.write(rowData(1, 7, 8L));
        write.write(rowData(1, 9, 10L));
        TableCommitImpl commit = table.newCommit(commitUser);
        commit.commit(0, write.prepareCommit(true, 0));
        write.close();
        commit.close();
        List<Split> splits =
                table.newReadBuilder()
                        .withBucketFilter(bucketId -> bucketId == 1)
                        .newScan()
                        .plan()
                        .splits();
        assertThat(splits.size()).isEqualTo(1);
        assertThat(((DataSplit) splits.get(0)).bucket()).isEqualTo(1);
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"avro", "orc", "parquet"})
    public void testReadRowType(String format) throws Exception {
        RowType writeType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "pt", DataTypes.INT()),
                        DataTypes.FIELD(1, "a", DataTypes.INT()),
                        DataTypes.FIELD(2, "f0", DataTypes.INT()),
                        DataTypes.FIELD(
                                3,
                                "f1",
                                DataTypes.ROW(
                                        DataTypes.FIELD(4, "f0", DataTypes.INT()),
                                        DataTypes.FIELD(5, "f1", DataTypes.INT()),
                                        DataTypes.FIELD(6, "f2", DataTypes.INT()))));

        FileStoreTable table =
                createFileStoreTable(conf -> conf.setString(FILE_FORMAT.key(), format), writeType);

        try (StreamTableWrite write = table.newWrite(commitUser);
                InnerTableCommit commit = table.newCommit(commitUser)) {
            write.write(GenericRow.of(0, 0, 0, GenericRow.of(10, 11, 12)));
            commit.commit(0, write.prepareCommit(true, 0));
        }

        RowType readType =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                3,
                                "f1",
                                DataTypes.ROW(
                                        DataTypes.FIELD(6, "f2", DataTypes.INT()),
                                        DataTypes.FIELD(4, "f0", DataTypes.INT()))));

        ReadBuilder readBuilder = table.newReadBuilder().withReadType(readType);
        List<Split> splits = readBuilder.newScan().plan().splits();
        List<InternalRow> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(splits.get(0))) {
            InternalRowSerializer serializer = new InternalRowSerializer(readType);
            reader.forEachRemaining(row -> result.add(serializer.copy(row)));
        }

        assertThat(result).containsExactly(GenericRow.of(GenericRow.of(12, 10)));
    }

    protected void innerTestWithShard(FileStoreTable table) throws Exception {
        StreamTableWrite write =
                table.newWrite(commitUser).withIOManager(IOManager.create(tempDir.toString()));
        write.write(rowData(1, 1, 2L));
        write.write(rowData(1, 3, 4L));
        write.write(rowData(1, 5, 6L));
        write.write(rowData(1, 7, 8L));
        write.write(rowData(1, 9, 10L));
        TableCommitImpl commit = table.newCommit(commitUser);
        commit.commit(0, write.prepareCommit(true, 0));
        write.close();
        commit.close();

        ReadBuilder readBuilder = table.newReadBuilder();

        List<Split> splits = new ArrayList<>();
        splits.addAll(readBuilder.withShard(0, 3).newScan().plan().splits());
        splits.addAll(readBuilder.withShard(1, 3).newScan().plan().splits());
        splits.addAll(readBuilder.withShard(2, 3).newScan().plan().splits());

        assertThat(getResult(readBuilder.newRead(), splits, BATCH_ROW_TO_STRING))
                .hasSameElementsAs(
                        Arrays.asList(
                                "1|3|4|binary|varbinary|mapKey:mapVal|multiset",
                                "1|9|10|binary|varbinary|mapKey:mapVal|multiset",
                                "1|1|2|binary|varbinary|mapKey:mapVal|multiset",
                                "1|5|6|binary|varbinary|mapKey:mapVal|multiset",
                                "1|7|8|binary|varbinary|mapKey:mapVal|multiset"));
    }

    @Test
    public void testBucketFilterConflictWithShard() throws Exception {
        String exceptionMessage = "Bucket filter and shard configuration cannot be used together";
        FileStoreTable table =
                createFileStoreTable(
                        conf -> {
                            conf.set(BUCKET, 5);
                            conf.set(BUCKET_KEY, "a");
                        });
        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(
                        () ->
                                table.newReadBuilder()
                                        .withBucketFilter(bucketId -> bucketId == 1)
                                        .withShard(0, 1)
                                        .newScan())
                .withMessageContaining(exceptionMessage);
        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(
                        () ->
                                table.newReadBuilder()
                                        .withShard(0, 1)
                                        .withBucketFilter(bucketId -> bucketId == 1)
                                        .newStreamScan())
                .withMessageContaining(exceptionMessage);
    }

    @Test
    public void testAbort() throws Exception {
        FileStoreTable table = createFileStoreTable(conf -> conf.set(BUCKET, 1));
        StreamTableWrite write = table.newWrite(commitUser);
        write.write(rowData(1, 2, 3L));
        List<CommitMessage> messages = write.prepareCommit(true, 0);
        TableCommitImpl commit = table.newCommit(commitUser);
        commit.abort(messages);

        FileStatus[] files =
                LocalFileIO.create()
                        .listStatus(new Path(table.location().toString() + "/pt=1/bucket-0"));
        assertThat(files).isEmpty();
        write.close();
        commit.close();
    }

    @Test
    public void testReadFilter() throws Exception {
        FileStoreTable table = createFileStoreTable();
        if (fileFormat(table.coreOptions()).getFormatIdentifier().equals("parquet")) {
            // TODO support parquet reader filter push down
            return;
        }

        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        write.write(rowData(1, 10, 100L));
        write.write(rowData(1, 20, 200L));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData(1, 30, 300L));
        write.write(rowData(1, 40, 400L));
        commit.commit(1, write.prepareCommit(true, 1));

        write.write(rowData(1, 50, 500L));
        write.write(rowData(1, 60, 600L));
        commit.commit(2, write.prepareCommit(true, 2));

        write.close();
        commit.close();

        PredicateBuilder builder = new PredicateBuilder(ROW_TYPE);
        List<Split> splits = toSplits(table.newSnapshotReader().read().dataSplits());
        TableRead read = table.newRead().withFilter(builder.equal(2, 300L));
        assertThat(getResult(read, splits, binaryRow(1), 0, BATCH_ROW_TO_STRING))
                .hasSameElementsAs(
                        Arrays.asList(
                                "1|30|300|binary|varbinary|mapKey:mapVal|multiset",
                                "1|40|400|binary|varbinary|mapKey:mapVal|multiset"));
    }

    @Test
    public void testPartitionEmptyWriter() throws Exception {
        FileStoreTable table = createFileStoreTable();
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        for (int i = 0; i < 4; i++) {
            // write lots of records, let compaction be slower
            for (int j = 0; j < 1000; j++) {
                write.write(rowData(1, 10 * i * j, 100L * i * j));
            }
            commit.commit(i, write.prepareCommit(false, i));
        }

        write.write(rowData(1, 40, 400L));
        List<CommitMessage> commit4 = write.prepareCommit(false, 4);
        // trigger compaction, but not wait it.

        if (((CommitMessageImpl) commit4.get(0)).compactIncrement().compactBefore().isEmpty()) {
            // commit4 is not a compaction commit
            // do compaction commit5 and compaction commit6
            write.write(rowData(2, 20, 200L));
            List<CommitMessage> commit5 = write.prepareCommit(true, 5);
            // wait compaction finish
            // commit5 should be a compaction commit

            write.write(rowData(1, 60, 600L));
            List<CommitMessage> commit6 = write.prepareCommit(true, 6);
            // if remove writer too fast, will see old files, do another compaction
            // then will be conflicts

            commit.commit(4, commit4);
            commit.commit(5, commit5);
            commit.commit(6, commit6);
        } else {
            // commit4 is a compaction commit
            // do compaction commit5
            write.write(rowData(2, 20, 200L));
            List<CommitMessage> commit5 = write.prepareCommit(true, 5);
            // wait compaction finish
            // commit5 should be a compaction commit

            commit.commit(4, commit4);
            commit.commit(5, commit5);
        }

        write.close();
        commit.close();
    }

    @Test
    public void testWriteWithoutCompactionAndExpiration() throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        conf -> {
                            conf.set(WRITE_ONLY, true);
                            // 'write-only' options will also skip expiration
                            // these options shouldn't have any effect
                            conf.set(SNAPSHOT_NUM_RETAINED_MIN, 3);
                            conf.set(SNAPSHOT_NUM_RETAINED_MAX, 3);
                        });
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);
        for (int i = 0; i < 10; i++) {
            write.write(rowData(1, 1, 100L));
            commit.commit(i, write.prepareCommit(true, i));
        }
        write.close();
        commit.close();

        List<DataFileMeta> files =
                table.newSnapshotReader().read().dataSplits().stream()
                        .flatMap(split -> split.dataFiles().stream())
                        .collect(Collectors.toList());
        for (DataFileMeta file : files) {
            assertThat(file.level()).isEqualTo(0);
        }

        SnapshotManager snapshotManager = newSnapshotManager(table.fileIO(), table.location());
        Long latestSnapshotId = snapshotManager.latestSnapshotId();
        assertThat(latestSnapshotId).isNotNull();
        for (int i = 1; i <= latestSnapshotId; i++) {
            Snapshot snapshot = snapshotManager.snapshot(i);
            assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);
        }
    }

    @Test
    public void testCopyWithLatestSchema() throws Exception {
        FileStoreTable table =
                createFileStoreTable(conf -> conf.set(SNAPSHOT_NUM_RETAINED_MAX, 100));
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        write.write(rowData(1, 10, 100L));
        write.write(rowData(1, 20, 200L));
        commit.commit(0, write.prepareCommit(true, 0));

        SchemaManager schemaManager = new SchemaManager(table.fileIO(), table.location());
        schemaManager.commitChanges(SchemaChange.addColumn("added", DataTypes.INT()));
        table = table.copyWithLatestSchema();
        assertThat(table.coreOptions().snapshotNumRetainMax()).isEqualTo(100);
        write = table.newWrite(commitUser);

        write.write(new JoinedRow(rowData(1, 30, 300L), GenericRow.of(3000)));
        write.write(new JoinedRow(rowData(1, 40, 400L), GenericRow.of(4000)));
        commit.commit(1, write.prepareCommit(true, 1));
        write.close();
        commit.close();

        List<Split> splits = table.newScan().plan().splits();
        TableRead read = table.newRead();
        Function<InternalRow, String> toString =
                rowData ->
                        BATCH_ROW_TO_STRING.apply(rowData)
                                + "|"
                                + (rowData.isNullAt(7) ? "null" : rowData.getInt(7));
        assertThat(getResult(read, splits, binaryRow(1), 0, toString))
                .hasSameElementsAs(
                        Arrays.asList(
                                "1|10|100|binary|varbinary|mapKey:mapVal|multiset|null",
                                "1|20|200|binary|varbinary|mapKey:mapVal|multiset|null",
                                "1|30|300|binary|varbinary|mapKey:mapVal|multiset|3000",
                                "1|40|400|binary|varbinary|mapKey:mapVal|multiset|4000"));
    }

    @Test
    public void testConsumerIdNotBlank() throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        options -> {
                            options.set(CoreOptions.CONSUMER_ID, "");
                            options.set(SNAPSHOT_NUM_RETAINED_MIN, 3);
                            options.set(SNAPSHOT_NUM_RETAINED_MAX, 3);
                        });

        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder();
        StreamTableWrite write = writeBuilder.newWrite();
        StreamTableCommit commit = writeBuilder.newCommit();

        ReadBuilder readBuilder = table.newReadBuilder();
        StreamTableScan scan = readBuilder.newStreamScan();
        TableRead read = readBuilder.newRead();

        write.write(rowData(1, 10, 100L));
        commit.commit(0, write.prepareCommit(true, 0));

        // assert can't set consumer-id to blank string
        assertThatThrownBy(() -> getResult(read, scan.plan().splits(), STREAMING_ROW_TO_STRING))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    public void testConsumeId() throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        options -> {
                            options.set(CoreOptions.CONSUMER_ID, "my_id");
                            options.set(SNAPSHOT_NUM_RETAINED_MIN, 3);
                            options.set(SNAPSHOT_NUM_RETAINED_MAX, 3);
                        });

        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder();
        StreamTableWrite write = writeBuilder.newWrite();
        StreamTableCommit commit = writeBuilder.newCommit();

        ReadBuilder readBuilder = table.newReadBuilder();
        StreamTableScan scan = readBuilder.newStreamScan();
        TableRead read = readBuilder.newRead();

        write.write(rowData(1, 10, 100L));
        commit.commit(0, write.prepareCommit(true, 0));

        List<String> result = getResult(read, scan.plan().splits(), STREAMING_ROW_TO_STRING);
        assertThat(result)
                .containsExactlyInAnyOrder("+1|10|100|binary|varbinary|mapKey:mapVal|multiset");

        write.write(rowData(1, 20, 200L));
        commit.commit(1, write.prepareCommit(true, 1));

        result = getResult(read, scan.plan().splits(), STREAMING_ROW_TO_STRING);
        assertThat(result)
                .containsExactlyInAnyOrder("+1|20|200|binary|varbinary|mapKey:mapVal|multiset");

        // checkpoint and notifyCheckpointComplete
        Long nextSnapshot = scan.checkpoint();
        scan.notifyCheckpointComplete(nextSnapshot);

        write.write(rowData(1, 30, 300L));
        commit.commit(2, write.prepareCommit(true, 2));

        // read again using consume id
        scan = readBuilder.newStreamScan();
        assertThat(scan.plan().splits()).isEmpty();
        result = getResult(read, scan.plan().splits(), STREAMING_ROW_TO_STRING);
        assertThat(result)
                .containsExactlyInAnyOrder("+1|30|300|binary|varbinary|mapKey:mapVal|multiset");

        // read again using consume id with ignore progress
        scan =
                table.copy(Collections.singletonMap(CONSUMER_IGNORE_PROGRESS.key(), "true"))
                        .newStreamScan();
        List<Split> splits = scan.plan().splits();
        result = getResult(read, splits, STREAMING_ROW_TO_STRING);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        "+1|10|100|binary|varbinary|mapKey:mapVal|multiset",
                        "+1|20|200|binary|varbinary|mapKey:mapVal|multiset",
                        "+1|30|300|binary|varbinary|mapKey:mapVal|multiset");

        // test snapshot expiration
        for (int i = 3; i <= 8; i++) {
            write.write(rowData(1, (i + 1) * 10, (i + 1) * 100L));
            commit.commit(i, write.prepareCommit(true, i));
        }

        // not expire
        result = getResult(read, scan.plan().splits(), STREAMING_ROW_TO_STRING);
        assertThat(result)
                .containsExactlyInAnyOrder("+1|40|400|binary|varbinary|mapKey:mapVal|multiset");
        write.close();
        commit.close();

        // expire consumer and then test snapshot expiration
        Thread.sleep(1000);
        table =
                table.copy(
                        Collections.singletonMap(
                                CoreOptions.CONSUMER_EXPIRATION_TIME.key(), "1 s"));

        // commit to trigger expiration
        writeBuilder = table.newStreamWriteBuilder();
        write = writeBuilder.newWrite();
        commit = writeBuilder.newCommit();

        write.write(rowData(1, 100, 1000L));
        commit.commit(9, write.prepareCommit(true, 9));

        StreamTableScan finalScan = scan;
        assertThatThrownBy(
                        () -> {
                            for (int i = 0; i < RANGE_CHECK_INTERVAL; i++) {
                                finalScan.plan();
                            }
                        })
                .satisfies(
                        anyCauseMatches(
                                OutOfRangeException.class, "The snapshot with id 5 has expired."));

        write.close();
        commit.close();
    }

    @Test
    public void testRollbackToSnapshotSkipNonExistentSnapshot() throws Exception {
        int commitTimes = ThreadLocalRandom.current().nextInt(100) + 5;
        FileStoreTable table = prepareRollbackTable(commitTimes);

        SnapshotManager snapshotManager = table.snapshotManager();
        table.snapshotManager()
                .commitLatestHint(checkNotNull(snapshotManager.latestSnapshotId()) + 100);
        table.rollbackTo(1L);
        ReadBuilder readBuilder = table.newReadBuilder();
        List<String> result =
                getResult(
                        readBuilder.newRead(),
                        readBuilder.newScan().plan().splits(),
                        BATCH_ROW_TO_STRING);
        assertThat(result)
                .containsExactlyInAnyOrder("0|0|0|binary|varbinary|mapKey:mapVal|multiset");

        assertRollbackTo(table, singletonList(1L), 1, 1, emptyList());
    }

    // All tags are after the rollback snapshot
    @Test
    public void testRollbackToSnapshotCase0() throws Exception {
        int commitTimes = ThreadLocalRandom.current().nextInt(100) + 5;
        FileStoreTable table = prepareRollbackTable(commitTimes);

        table.createTag("test1", commitTimes);
        table.createTag("test2", commitTimes - 1);
        table.createTag("test3", commitTimes - 2);

        table.rollbackTo(1);
        ReadBuilder readBuilder = table.newReadBuilder();
        List<String> result =
                getResult(
                        readBuilder.newRead(),
                        readBuilder.newScan().plan().splits(),
                        BATCH_ROW_TO_STRING);
        assertThat(result)
                .containsExactlyInAnyOrder("0|0|0|binary|varbinary|mapKey:mapVal|multiset");

        assertRollbackTo(table, singletonList(1L), 1, 1, emptyList());
    }

    // One tag is at the rollback snapshot and others are after it
    @Test
    public void testRollbackToSnapshotCase1() throws Exception {
        int commitTimes = ThreadLocalRandom.current().nextInt(100) + 5;
        FileStoreTable table = prepareRollbackTable(commitTimes);

        table.createTag("test1", commitTimes);
        table.createTag("test2", commitTimes - 1);
        table.createTag("test3", 1);

        table.rollbackTo(1);
        ReadBuilder readBuilder = table.newReadBuilder();
        List<String> result =
                getResult(
                        readBuilder.newRead(),
                        readBuilder.newScan().plan().splits(),
                        BATCH_ROW_TO_STRING);
        assertThat(result)
                .containsExactlyInAnyOrder("0|0|0|binary|varbinary|mapKey:mapVal|multiset");

        // read tag test3
        table = table.copy(Collections.singletonMap(CoreOptions.SCAN_TAG_NAME.key(), "test3"));
        readBuilder = table.newReadBuilder();
        result =
                getResult(
                        readBuilder.newRead(),
                        readBuilder.newScan().plan().splits(),
                        BATCH_ROW_TO_STRING);
        assertThat(result)
                .containsExactlyInAnyOrder("0|0|0|binary|varbinary|mapKey:mapVal|multiset");

        assertRollbackTo(table, singletonList(1L), 1, 1, singletonList("test3"));
    }

    // One tag is before the rollback snapshot and others are after it
    @Test
    public void testRollbackToSnapshotCase2() throws Exception {
        int commitTimes = ThreadLocalRandom.current().nextInt(100) + 5;
        FileStoreTable table = prepareRollbackTable(commitTimes);

        table.createTag("test1", commitTimes);
        table.createTag("test2", commitTimes - 1);
        table.createTag("test3", 1);

        table.rollbackTo(2);
        ReadBuilder readBuilder = table.newReadBuilder();
        List<String> result =
                getResult(
                        readBuilder.newRead(),
                        readBuilder.newScan().plan().splits(),
                        BATCH_ROW_TO_STRING);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        "0|0|0|binary|varbinary|mapKey:mapVal|multiset",
                        "1|10|100|binary|varbinary|mapKey:mapVal|multiset");

        // read tag test3
        table = table.copy(Collections.singletonMap(CoreOptions.SCAN_TAG_NAME.key(), "test3"));
        readBuilder = table.newReadBuilder();
        result =
                getResult(
                        readBuilder.newRead(),
                        readBuilder.newScan().plan().splits(),
                        BATCH_ROW_TO_STRING);
        assertThat(result)
                .containsExactlyInAnyOrder("0|0|0|binary|varbinary|mapKey:mapVal|multiset");

        assertRollbackTo(table, Arrays.asList(1L, 2L), 1, 2, singletonList("test3"));
    }

    @ParameterizedTest(name = "expire snapshots = {0}")
    @ValueSource(booleans = {true, false})
    public void testRollbackToTag(boolean expire) throws Exception {
        int commitTimes = ThreadLocalRandom.current().nextInt(100) + 5;
        FileStoreTable table = prepareRollbackTable(commitTimes);

        table.createTag("test1", 1);
        table.createTag("test2", commitTimes - 3);
        table.createTag("test3", commitTimes - 1);

        if (expire) {
            // expire snapshots
            Options options = new Options();
            options.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN, 5);
            options.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX, 5);
            options.set(SNAPSHOT_EXPIRE_LIMIT, Integer.MAX_VALUE);
            options.set(CHANGELOG_NUM_RETAINED_MIN, 5);
            options.set(CHANGELOG_NUM_RETAINED_MAX, 5);
            table.copy(options.toMap()).newCommit("").expireSnapshots();
        }

        table.rollbackTo("test1");
        ReadBuilder readBuilder = table.newReadBuilder();
        List<String> result =
                getResult(
                        readBuilder.newRead(),
                        readBuilder.newScan().plan().splits(),
                        BATCH_ROW_TO_STRING);
        assertThat(result)
                .containsExactlyInAnyOrder("0|0|0|binary|varbinary|mapKey:mapVal|multiset");

        // read tag test1
        table = table.copy(Collections.singletonMap(CoreOptions.SCAN_TAG_NAME.key(), "test1"));
        readBuilder = table.newReadBuilder();
        result =
                getResult(
                        readBuilder.newRead(),
                        readBuilder.newScan().plan().splits(),
                        BATCH_ROW_TO_STRING);
        assertThat(result)
                .containsExactlyInAnyOrder("0|0|0|binary|varbinary|mapKey:mapVal|multiset");

        assertRollbackTo(table, singletonList(1L), 1, 1, singletonList("test1"));
    }

    @Test
    public void testRollbackToTagFromSnapshotId() throws Exception {
        int commitTimes = ThreadLocalRandom.current().nextInt(100) + 5;
        FileStoreTable table = prepareRollbackTable(commitTimes);

        table.createTag("test", 1);

        // expire snapshots
        Options options = new Options();
        options.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN, 5);
        options.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX, 5);
        options.set(SNAPSHOT_EXPIRE_LIMIT, Integer.MAX_VALUE);
        options.set(CHANGELOG_NUM_RETAINED_MIN, 5);
        options.set(CHANGELOG_NUM_RETAINED_MAX, 5);
        table.copy(options.toMap()).newCommit("").expireSnapshots();

        table.rollbackTo(1);
        ReadBuilder readBuilder = table.newReadBuilder();
        List<String> result =
                getResult(
                        readBuilder.newRead(),
                        readBuilder.newScan().plan().splits(),
                        BATCH_ROW_TO_STRING);
        assertThat(result)
                .containsExactlyInAnyOrder("0|0|0|binary|varbinary|mapKey:mapVal|multiset");

        assertRollbackTo(table, singletonList(1L), 1, 1, singletonList("test"));
    }

    private FileStoreTable prepareRollbackTable(int commitTimes) throws Exception {
        FileStoreTable table = createFileStoreTable();
        return prepareRollbackTable(commitTimes, table);
    }

    protected FileStoreTable prepareRollbackTable(int commitTimes, FileStoreTable table)
            throws Exception {
        table =
                table.copy(
                        Collections.singletonMap(SNAPSHOT_CLEAN_EMPTY_DIRECTORIES.key(), "true"));
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        for (int i = 0; i < commitTimes; i++) {
            write.write(rowData(i, 10 * i, 100L * i));
            commit.commit(i, write.prepareCommit(false, i));
        }
        write.close();
        commit.close();

        return table;
    }

    @Test
    public void testCreateTag() throws Exception {
        FileStoreTable table = createFileStoreTable();

        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            // snapshot 1
            write.write(rowData(1, 10, 100L));
            commit.commit(0, write.prepareCommit(false, 1));
            // snapshot 2
            write.write(rowData(2, 20, 200L));
            commit.commit(1, write.prepareCommit(false, 2));
        }

        table.createTag("test-tag", 2);

        // verify that tag file exist
        TagManager tagManager = new TagManager(table.fileIO(), table.location());
        assertThat(tagManager.tagExists("test-tag")).isTrue();

        // verify that test-tag is equal to snapshot 2
        Snapshot tagged = tagManager.getOrThrow("test-tag").trimToSnapshot();
        Snapshot snapshot2 = table.snapshotManager().snapshot(2);
        assertThat(tagged.equals(snapshot2)).isTrue();
    }

    @Test
    public void testCreateTagOnExpiredSnapshot() throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        conf -> {
                            conf.set(SNAPSHOT_NUM_RETAINED_MAX, 1);
                            conf.set(SNAPSHOT_NUM_RETAINED_MIN, 1);
                        });
        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            // snapshot 1
            write.write(rowData(1, 10, 100L));
            commit.commit(0, write.prepareCommit(false, 1));
            table.createTag("test-tag", 1);
            // verify that tag file exist
            TagManager tagManager = new TagManager(table.fileIO(), table.location());
            assertThat(tagManager.tagExists("test-tag")).isTrue();
            // verify that test-tag is equal to snapshot 1
            Snapshot tagged = tagManager.getOrThrow("test-tag").trimToSnapshot();
            Snapshot snapshot1 = table.snapshotManager().snapshot(1);
            assertThat(tagged.equals(snapshot1)).isTrue();
            // snapshot 2
            write.write(rowData(2, 20, 200L));
            commit.commit(1, write.prepareCommit(false, 2));
            SnapshotManager snapshotManager = newSnapshotManager(table.fileIO(), table.location());
            // The snapshot 1 is expired.
            assertThat(snapshotManager.snapshotExists(1)).isFalse();
            table.createTag("test-tag-2", 1);
            // verify that tag file exist
            assertThat(tagManager.tagExists("test-tag-2")).isTrue();
            // verify that test-tag is equal to snapshot 1
            Snapshot tag2 = tagManager.getOrThrow("test-tag-2").trimToSnapshot();
            assertThat(tag2.equals(snapshot1)).isTrue();
        }
    }

    @Test
    public void testCreateSameTagName() throws Exception {
        FileStoreTable table = createFileStoreTable();
        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            // snapshot 1
            write.write(rowData(1, 10, 100L));
            commit.commit(0, write.prepareCommit(false, 1));
            // snapshot 2
            write.write(rowData(1, 10, 100L));
            commit.commit(1, write.prepareCommit(false, 2));
            TagManager tagManager = new TagManager(table.fileIO(), table.location());
            table.createTag("test-tag", 1);
            // verify that tag file exist
            assertThat(tagManager.tagExists("test-tag")).isTrue();
            // Create again failed if tag existed
            Assertions.assertThatThrownBy(() -> table.createTag("test-tag", 1))
                    .hasMessageContaining("Tag 'test-tag' already exists.");
            Assertions.assertThatThrownBy(() -> table.createTag("test-tag", 2))
                    .hasMessageContaining("Tag 'test-tag' already exists.");
        }
    }

    @Test
    public void testCreateBranch() throws Exception {
        FileStoreTable table = createFileStoreTable();
        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            // snapshot 1
            write.write(rowData(1, 10, 100L));
            commit.commit(0, write.prepareCommit(false, 1));
            // snapshot 2
            write.write(rowData(2, 20, 200L));
            commit.commit(1, write.prepareCommit(false, 2));
        }

        table.createTag("test-tag", 2);

        // verify that tag file exist
        TagManager tagManager = new TagManager(table.fileIO(), table.location());
        assertThat(tagManager.tagExists("test-tag")).isTrue();

        // verify that test-tag is equal to snapshot 2
        Snapshot tagged = tagManager.getOrThrow("test-tag").trimToSnapshot();
        Snapshot snapshot2 = table.snapshotManager().snapshot(2);
        assertThat(tagged.equals(snapshot2)).isTrue();

        table.createBranch("test-branch", "test-tag");

        // verify that branch file exist
        BranchManager branchManager = table.branchManager();
        assertThat(branchManager.branchExists("test-branch")).isTrue();

        // verify test-tag in test-branch is equal to snapshot 2
        Snapshot branchTag =
                SnapshotManager.fromPath(
                        table.fileIO(),
                        tagManager.copyWithBranch("test-branch").tagPath("test-tag"));
        assertThat(branchTag.equals(snapshot2)).isTrue();

        // verify snapshot in test-branch is equal to snapshot 2
        SnapshotManager snapshotManager =
                newSnapshotManager(table.fileIO(), table.location(), "test-branch");
        Snapshot branchSnapshot =
                SnapshotManager.fromPath(table.fileIO(), snapshotManager.snapshotPath(2));
        assertThat(branchSnapshot.equals(snapshot2)).isTrue();

        // verify schema in test-branch is equal to schema 0
        SchemaManager schemaManager =
                new SchemaManager(table.fileIO(), table.location(), "test-branch");
        TableSchema branchSchema =
                SchemaManager.fromPath(table.fileIO(), schemaManager.toSchemaPath(0));
        TableSchema schema0 = schemaManager.schema(0);
        assertThat(branchSchema.equals(schema0)).isTrue();
    }

    @Test
    public void testUnsupportedBranchName() throws Exception {
        FileStoreTable table = createFileStoreTable();

        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(1, 10, 100L));
            commit.commit(0, write.prepareCommit(false, 1));
        }

        table.createTag("test-tag", 1);
        table.createBranch("branch0", "test-tag");

        assertThatThrownBy(() -> table.createBranch("main", "tag1"))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Branch name 'main' is the default branch and cannot be used."));

        assertThatThrownBy(() -> table.createBranch("branch-1", "tag1"))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class, "Tag 'tag1' doesn't exist."));

        assertThatThrownBy(() -> table.createBranch("branch0", "test-tag"))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Branch name 'branch0' already exists."));

        assertThatThrownBy(() -> table.createBranch("", "test-tag"))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                String.format("Branch name '%s' is blank", "")));

        assertThatThrownBy(() -> table.createBranch("10", "test-tag"))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Branch name cannot be pure numeric string but is '10'."));
    }

    @Test
    public void testDeleteBranch() throws Exception {
        FileStoreTable table = createFileStoreTable();

        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(1, 10, 100L));
            commit.commit(0, write.prepareCommit(false, 1));
        }

        table.createTag("tag1", 1);
        table.createBranch("branch1", "tag1");
        table.deleteBranch("branch1");

        // verify that branch file not exist
        BranchManager branchManager = table.branchManager();
        assertThat(branchManager.branchExists("branch1")).isFalse();

        assertThatThrownBy(() -> table.deleteBranch("branch1"))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Branch name 'branch1' doesn't exist."));

        // test delete fallback branch
        table.createBranch("fallback");

        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put("scan.fallback-branch", "fallback");
        FileStoreTable table1 = table.copy(dynamicOptions);
        assertThatThrownBy(() -> table1.deleteBranch("fallback"))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "can not delete the fallback branch. "
                                        + "branchName to be deleted is fallback. you have set 'scan.fallback-branch' = 'fallback'. "
                                        + "you should reset 'scan.fallback-branch' before deleting this branch."));

        table.deleteBranch("fallback");
    }

    @Test
    public void testFastForward() throws Exception {
        FileStoreTable table = createFileStoreTable();
        generateBranch(table);
        FileStoreTable tableBranch = createBranchTable(BRANCH_NAME);

        // Verify branch1 and the main branch have the same data
        assertThat(
                        getResult(
                                tableBranch.newRead(),
                                toSplits(tableBranch.newSnapshotReader().read().dataSplits()),
                                BATCH_ROW_TO_STRING))
                .containsExactlyInAnyOrder("0|0|0|binary|varbinary|mapKey:mapVal|multiset");

        // Test for unsupported branch name
        assertThatThrownBy(() -> table.fastForward("test-branch"))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Branch name 'test-branch' doesn't exist."));

        assertThatThrownBy(() -> table.fastForward("main"))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Branch name 'main' do not use in fast-forward."));

        // Write data to branch1
        try (StreamTableWrite write = tableBranch.newWrite(commitUser);
                StreamTableCommit commit = tableBranch.newCommit(commitUser)) {
            write.write(rowData(2, 20, 200L));
            commit.commit(1, write.prepareCommit(false, 2));
        }

        // Validate data in branch1
        assertThat(
                        getResult(
                                tableBranch.newRead(),
                                toSplits(tableBranch.newSnapshotReader().read().dataSplits()),
                                BATCH_ROW_TO_STRING))
                .containsExactlyInAnyOrder(
                        "0|0|0|binary|varbinary|mapKey:mapVal|multiset",
                        "2|20|200|binary|varbinary|mapKey:mapVal|multiset");

        // Validate data in main branch not changed
        assertThat(
                        getResult(
                                table.newRead(),
                                toSplits(table.newSnapshotReader().read().dataSplits()),
                                BATCH_ROW_TO_STRING))
                .containsExactlyInAnyOrder("0|0|0|binary|varbinary|mapKey:mapVal|multiset");

        // Fast-forward branch1 to main branch
        table.fastForward(BRANCH_NAME);

        // After fast-forward branch1, verify branch1 and the main branch have the same data
        assertThat(
                        getResult(
                                table.newRead(),
                                toSplits(table.newSnapshotReader().read().dataSplits()),
                                BATCH_ROW_TO_STRING))
                .containsExactlyInAnyOrder(
                        "0|0|0|binary|varbinary|mapKey:mapVal|multiset",
                        "2|20|200|binary|varbinary|mapKey:mapVal|multiset");

        // verify snapshot in branch1 and main branch is same
        SnapshotManager snapshotManager = newSnapshotManager(table.fileIO(), table.location());
        Snapshot branchSnapshot =
                SnapshotManager.fromPath(
                        table.fileIO(),
                        snapshotManager.copyWithBranch(BRANCH_NAME).snapshotPath(2));
        Snapshot snapshot =
                SnapshotManager.fromPath(table.fileIO(), snapshotManager.snapshotPath(2));
        assertThat(branchSnapshot.equals(snapshot)).isTrue();

        // verify schema in branch1 and main branch is same
        SchemaManager schemaManager = new SchemaManager(table.fileIO(), table.location());
        TableSchema branchSchema =
                SchemaManager.fromPath(
                        table.fileIO(), schemaManager.copyWithBranch(BRANCH_NAME).toSchemaPath(0));
        TableSchema schema0 = schemaManager.schema(0);
        assertThat(branchSchema.equals(schema0)).isTrue();

        // Write two rows data to branch1 again
        try (StreamTableWrite write = tableBranch.newWrite(commitUser);
                StreamTableCommit commit = tableBranch.newCommit(commitUser)) {
            write.write(rowData(3, 30, 300L));
            write.write(rowData(4, 40, 400L));
            commit.commit(2, write.prepareCommit(false, 3));
        }

        // Verify data in branch1
        assertThat(
                        getResult(
                                tableBranch.newRead(),
                                toSplits(tableBranch.newSnapshotReader().read().dataSplits()),
                                BATCH_ROW_TO_STRING))
                .containsExactlyInAnyOrder(
                        "0|0|0|binary|varbinary|mapKey:mapVal|multiset",
                        "2|20|200|binary|varbinary|mapKey:mapVal|multiset",
                        "3|30|300|binary|varbinary|mapKey:mapVal|multiset",
                        "4|40|400|binary|varbinary|mapKey:mapVal|multiset");

        // Verify data in main branch not changed
        assertThat(
                        getResult(
                                table.newRead(),
                                toSplits(table.newSnapshotReader().read().dataSplits()),
                                BATCH_ROW_TO_STRING))
                .containsExactlyInAnyOrder(
                        "0|0|0|binary|varbinary|mapKey:mapVal|multiset",
                        "2|20|200|binary|varbinary|mapKey:mapVal|multiset");

        // Fast-forward branch1 to main branch again
        table.fastForward("branch1");

        // Verify data in main branch is same to branch1
        assertThat(
                        getResult(
                                table.newRead(),
                                toSplits(table.newSnapshotReader().read().dataSplits()),
                                BATCH_ROW_TO_STRING))
                .containsExactlyInAnyOrder(
                        "0|0|0|binary|varbinary|mapKey:mapVal|multiset",
                        "2|20|200|binary|varbinary|mapKey:mapVal|multiset",
                        "3|30|300|binary|varbinary|mapKey:mapVal|multiset",
                        "4|40|400|binary|varbinary|mapKey:mapVal|multiset");
    }

    @Test
    public void testUnsupportedTagName() throws Exception {
        FileStoreTable table = createFileStoreTable();

        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(1, 10, 100L));
            commit.commit(0, write.prepareCommit(false, 1));
        }

        assertThatThrownBy(() -> table.createTag("", 1))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class, "Tag name shouldn't be blank"));
    }

    @Test
    public void testDeleteTag() throws Exception {
        FileStoreTable table = createFileStoreTable();

        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(1, 10, 100L));
            commit.commit(0, write.prepareCommit(false, 1));
        }

        table.createTag("tag1", 1);
        table.deleteTag("tag1");

        assertThat(table.tagManager().tags().containsValue("tag1")).isFalse();
    }

    @Test
    @Timeout(120)
    public void testAsyncExpireExecutionMode() throws Exception {
        FileStoreTable table = createFileStoreTable();

        Map<String, String> options = new HashMap<>();
        options.put(SNAPSHOT_EXPIRE_EXECUTION_MODE.key(), ExpireExecutionMode.ASYNC.toString());
        options.put(SNAPSHOT_NUM_RETAINED_MIN.key(), "1");
        options.put(SNAPSHOT_NUM_RETAINED_MAX.key(), "1");
        options.put(CHANGELOG_NUM_RETAINED_MIN.key(), "1");
        options.put(CHANGELOG_NUM_RETAINED_MAX.key(), "1");
        options.put(SNAPSHOT_EXPIRE_LIMIT.key(), "2");

        TableCommitImpl commit = table.copy(options).newCommit(commitUser);
        ExecutorService executor = commit.getMaintainExecutor();
        CountDownLatch before = new CountDownLatch(1);
        CountDownLatch after = new CountDownLatch(1);

        executor.execute(
                () -> {
                    try {
                        before.await();
                    } catch (Exception ignore) {
                        // ignore
                    }
                });

        try (StreamTableWrite write = table.newWrite(commitUser)) {
            for (int i = 0; i < 10; i++) {
                write.write(rowData(i, 10 * i, 100L * i));
                commit.commit(i, write.prepareCommit(false, i));
            }
        }

        executor.execute(after::countDown);

        SnapshotManager snapshotManager = table.snapshotManager();
        long latestSnapshotId = snapshotManager.latestSnapshotId();

        FileStore<?> store = table.store();
        Set<Path> filesInUse =
                TestFileStore.getFilesInUse(
                        latestSnapshotId,
                        snapshotManager,
                        table.changelogManager(),
                        table.fileIO(),
                        store.pathFactory(),
                        store.manifestListFactory().create(),
                        store.manifestFileFactory().create());

        List<Path> unusedFileList =
                Arrays.stream(table.fileIO().listFiles(table.location(), true))
                        .map(FileStatus::getPath)
                        .filter(p -> !p.getName().startsWith("snapshot"))
                        .filter(p -> !p.getName().startsWith("schema"))
                        .filter(p -> !p.getName().equals(LATEST))
                        .filter(p -> !p.getName().equals(EARLIEST))
                        .map(
                                p ->
                                        new Path(
                                                TraceableFileIO.SCHEME
                                                        + ":"
                                                        + p.toString().replace("file:", "")))
                        .filter(p -> !filesInUse.contains(p))
                        .collect(Collectors.toList());

        // no expire happens, all files are preserved
        for (int i = 1; i <= latestSnapshotId; i++) {
            assertThat(snapshotManager.snapshotExists(i)).isTrue();
        }
        for (Path file : unusedFileList) {
            FileStoreTestUtils.assertPathExists(table.fileIO(), file);
        }

        // waiting for all expire, only keeping files in use.
        before.countDown();
        after.await();

        for (int i = 1; i < latestSnapshotId - 1; i++) {
            assertThat(snapshotManager.snapshotExists(i)).isFalse();
        }
        for (Path file : unusedFileList) {
            FileStoreTestUtils.assertPathNotExists(table.fileIO(), file);
        }
        assertThat(snapshotManager.snapshotExists(latestSnapshotId)).isTrue();
        assertThat(snapshotManager.earliestSnapshotId()).isEqualTo(latestSnapshotId);
        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(latestSnapshotId);

        commit.close();
    }

    @Test
    public void testBatchWriteAsyncExpireFallbackToSync() throws Exception {
        // configure table to async expire but retain only last snapshot
        Map<String, String> opts = new HashMap<>();
        opts.put(SNAPSHOT_EXPIRE_EXECUTION_MODE.key(), ExpireExecutionMode.ASYNC.toString());
        opts.put(SNAPSHOT_NUM_RETAINED_MIN.key(), "1");
        opts.put(SNAPSHOT_NUM_RETAINED_MAX.key(), "1");
        opts.put(SNAPSHOT_EXPIRE_LIMIT.key(), "100");

        FileStoreTable table = createFileStoreTable(conf -> {});
        table = table.copy(opts);

        SnapshotManager sm = table.snapshotManager();
        AtomicLong lastId = new AtomicLong(0);

        // perform multiple batch commits; expiration should run synchronously after each commit
        for (int i = 0; i < 3; i++) {
            BatchWriteBuilder builder = table.newBatchWriteBuilder();
            try (BatchTableWrite write = builder.newWrite();
                    BatchTableCommit commit = builder.newCommit()) {
                write.write(rowData(i, i * 10, i * 100L));
                commit.commit(write.prepareCommit());
            }

            long latest = sm.latestSnapshotId();
            assertThat(latest).isGreaterThan(0);

            if (lastId.get() > 0) {
                // since retain min/max = 1, previous snapshot must have been expired synchronously
                assertThat(sm.snapshotExists(lastId.get()))
                        .as("previous snapshot should be expired synchronously in batch mode")
                        .isFalse();
                assertThat(sm.earliestSnapshotId()).isEqualTo(latest);
            }
            lastId.set(latest);
        }
    }

    @Test
    @Timeout(120)
    public void testExpireWithLimit() throws Exception {
        FileStoreTable table = createFileStoreTable();

        Map<String, String> options = new HashMap<>();
        options.put(SNAPSHOT_EXPIRE_EXECUTION_MODE.key(), ExpireExecutionMode.ASYNC.toString());
        options.put(SNAPSHOT_NUM_RETAINED_MIN.key(), "1");
        options.put(SNAPSHOT_NUM_RETAINED_MAX.key(), "1");
        options.put(SNAPSHOT_EXPIRE_LIMIT.key(), "2");

        table = table.copy(options);
        TableCommitImpl commit =
                table.copy(Collections.singletonMap(WRITE_ONLY.key(), "true"))
                        .newCommit(commitUser);
        TableCommitImpl expire = table.newCommit(commitUser);

        try (StreamTableWrite write = table.newWrite(commitUser)) {
            for (int i = 0; i < 10; i++) {
                write.write(rowData(i, 10 * i, 100L * i));
                commit.commit(i, write.prepareCommit(false, i));
            }
        }

        SnapshotManager snapshotManager = table.snapshotManager();
        List<Snapshot> remainingSnapshot = new ArrayList<>();
        snapshotManager.snapshots().forEachRemaining(remainingSnapshot::add);
        long latestSnapshotId = snapshotManager.latestSnapshotId();
        int index = 0;

        // trigger the first expire and the first two snapshots expired
        expire.expireSnapshots();
        assertThat(snapshotManager.snapshotExists(remainingSnapshot.get(index++).id())).isFalse();
        assertThat(snapshotManager.snapshotExists(remainingSnapshot.get(index++).id())).isFalse();
        for (int i = index; i < remainingSnapshot.size(); i++) {
            assertThat(snapshotManager.snapshotExists(remainingSnapshot.get(i).id())).isTrue();
        }
        assertThat(snapshotManager.earliestSnapshotId())
                .isEqualTo(remainingSnapshot.get(index).id());

        // trigger the second expire and the second two snapshots expired
        expire.expireSnapshots();
        assertThat(snapshotManager.snapshotExists(remainingSnapshot.get(index++).id())).isFalse();
        assertThat(snapshotManager.snapshotExists(remainingSnapshot.get(index++).id())).isFalse();
        for (int i = index; i < remainingSnapshot.size(); i++) {
            assertThat(snapshotManager.snapshotExists(remainingSnapshot.get(i).id())).isTrue();
        }
        assertThat(snapshotManager.earliestSnapshotId())
                .isEqualTo(remainingSnapshot.get(index).id());

        // trigger all remaining expires and only the last snapshot remaining
        for (int i = 0; i < 5; i++) {
            expire.expireSnapshots();
        }

        for (int i = 0; i < remainingSnapshot.size() - 1; i++) {
            assertThat(snapshotManager.snapshotExists(remainingSnapshot.get(i).id())).isFalse();
        }
        assertThat(snapshotManager.snapshotExists(latestSnapshotId)).isTrue();
        assertThat(snapshotManager.earliestSnapshotId()).isEqualTo(latestSnapshotId);
        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(latestSnapshotId);

        commit.close();
    }

    @Test
    public void testBranchWriteAndRead() throws Exception {
        FileStoreTable table = createFileStoreTable();

        generateBranch(table);

        FileStoreTable tableBranch = createBranchTable(BRANCH_NAME);
        // Write data to branch1
        try (StreamTableWrite write = tableBranch.newWrite(commitUser);
                StreamTableCommit commit = tableBranch.newCommit(commitUser)) {
            write.write(rowData(2, 20, 200L));
            commit.commit(1, write.prepareCommit(false, 2));
        }

        // Validate data in main branch
        assertThat(
                        getResult(
                                table.newRead(),
                                toSplits(table.newSnapshotReader().read().dataSplits()),
                                BATCH_ROW_TO_STRING))
                .containsExactlyInAnyOrder("0|0|0|binary|varbinary|mapKey:mapVal|multiset");

        // Validate data in branch1
        assertThat(
                        getResult(
                                tableBranch.newRead(),
                                toSplits(tableBranch.newSnapshotReader().read().dataSplits()),
                                BATCH_ROW_TO_STRING))
                .containsExactlyInAnyOrder(
                        "0|0|0|binary|varbinary|mapKey:mapVal|multiset",
                        "2|20|200|binary|varbinary|mapKey:mapVal|multiset");

        // Write two rows data to branch1 again
        try (StreamTableWrite write = tableBranch.newWrite(commitUser);
                StreamTableCommit commit = tableBranch.newCommit(commitUser)) {
            write.write(rowData(3, 30, 300L));
            write.write(rowData(4, 40, 400L));
            commit.commit(2, write.prepareCommit(false, 3));
        }

        // Validate data in main branch
        assertThat(
                        getResult(
                                table.newRead(),
                                toSplits(table.newSnapshotReader().read().dataSplits()),
                                BATCH_ROW_TO_STRING))
                .containsExactlyInAnyOrder("0|0|0|binary|varbinary|mapKey:mapVal|multiset");

        // Verify data in branch1
        assertThat(
                        getResult(
                                tableBranch.newRead(),
                                toSplits(tableBranch.newSnapshotReader().read().dataSplits()),
                                BATCH_ROW_TO_STRING))
                .containsExactlyInAnyOrder(
                        "0|0|0|binary|varbinary|mapKey:mapVal|multiset",
                        "2|20|200|binary|varbinary|mapKey:mapVal|multiset",
                        "3|30|300|binary|varbinary|mapKey:mapVal|multiset",
                        "4|40|400|binary|varbinary|mapKey:mapVal|multiset");
    }

    @Test
    public void testDataSplitNotIncludeDvFilesWhenStreamingRead() throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        options -> {
                            options.set(DELETION_VECTORS_ENABLED, true);
                            options.set(WRITE_ONLY, true);
                        });

        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            for (int i = 0; i < 10; i++) {
                write.write(rowData(i, 10 * i, 100L * i));
                commit.commit(i, write.prepareCommit(false, i));
            }
        }

        List<Split> splits =
                toSplits(table.newSnapshotReader().withMode(ScanMode.DELTA).read().dataSplits());

        for (Split split : splits) {
            DataSplit dataSplit = (DataSplit) split;
            Assertions.assertThat(dataSplit.deletionFiles().isPresent()).isFalse();
        }
    }

    @Test
    public void testDataSplitNotIncludeDvFilesWhenStreamingReadChanges() throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        options -> {
                            options.set(DELETION_VECTORS_ENABLED, true);
                            options.set(WRITE_ONLY, true);
                        });

        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            for (int i = 0; i < 10; i++) {
                write.write(rowData(i, 10 * i, 100L * i));
                commit.commit(i, write.prepareCommit(false, i));
            }
        }

        List<Split> splits =
                toSplits(
                        table.newSnapshotReader()
                                .withMode(ScanMode.DELTA)
                                .readChanges()
                                .dataSplits());

        for (Split split : splits) {
            DataSplit dataSplit = (DataSplit) split;
            Assertions.assertThat(dataSplit.deletionFiles().isPresent()).isFalse();
        }
    }

    public static List<String> getResult(
            TableRead read,
            List<Split> splits,
            BinaryRow partition,
            int bucket,
            Function<InternalRow, String> rowDataToString)
            throws Exception {
        return getResult(read, getSplitsFor(splits, partition, bucket), rowDataToString);
    }

    public static List<String> getResult(
            TableRead read, List<Split> splits, Function<InternalRow, String> rowDataToString)
            throws Exception {
        List<ReaderSupplier<InternalRow>> readers = new ArrayList<>();
        for (Split split : splits) {
            readers.add(() -> read.createReader(split));
        }
        RecordReader<InternalRow> recordReader = ConcatRecordReader.create(readers);
        RecordReaderIterator<InternalRow> iterator = new RecordReaderIterator<>(recordReader);
        List<String> result = new ArrayList<>();
        while (iterator.hasNext()) {
            InternalRow rowData = iterator.next();
            result.add(rowDataToString.apply(rowData));
        }
        iterator.close();
        return result;
    }

    private static List<Split> getSplitsFor(List<Split> splits, BinaryRow partition, int bucket) {
        List<Split> result = new ArrayList<>();
        for (Split split : splits) {
            DataSplit dataSplit = (DataSplit) split;
            if (dataSplit.partition().equals(partition) && dataSplit.bucket() == bucket) {
                result.add(split);
            }
        }
        return result;
    }

    protected BinaryRow binaryRow(int a) {
        BinaryRow b = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(b);
        writer.writeInt(0, a);
        writer.complete();
        return b;
    }

    protected GenericRow rowData(Object... values) {
        return GenericRow.of(
                values[0],
                values[1],
                values[2],
                "binary".getBytes(),
                "varbinary".getBytes(),
                new GenericMap(
                        Collections.singletonMap(
                                BinaryString.fromString("mapKey"),
                                BinaryString.fromString("mapVal"))),
                new GenericMap(Collections.singletonMap(BinaryString.fromString("multiset"), 1)));
    }

    protected GenericRow rowDataWithKind(RowKind rowKind, Object... values) {
        return GenericRow.ofKind(
                rowKind,
                values[0],
                values[1],
                values[2],
                "binary".getBytes(),
                "varbinary".getBytes(),
                new GenericMap(
                        Collections.singletonMap(
                                BinaryString.fromString("mapKey"),
                                BinaryString.fromString("mapVal"))),
                new GenericMap(Collections.singletonMap(BinaryString.fromString("multiset"), 1)));
    }

    protected FileStoreTable createFileStoreTable(int numOfBucket) throws Exception {
        return createFileStoreTable(conf -> conf.set(BUCKET, numOfBucket));
    }

    protected FileStoreTable createBranchTable(String branch) throws Exception {
        FileStoreTable table = createFileStoreTable();
        return table.switchToBranch(branch);
    }

    protected FileStoreTable createFileStoreTable() throws Exception {
        return createFileStoreTable(1);
    }

    protected FileStoreTable createFileStoreTable(Consumer<Options> configure) throws Exception {
        return createFileStoreTable(configure, ROW_TYPE);
    }

    protected abstract FileStoreTable createFileStoreTable(
            Consumer<Options> configure, RowType rowType) throws Exception;

    protected List<Split> toSplits(List<DataSplit> dataSplits) {
        return new ArrayList<>(dataSplits);
    }

    // create a branch which named branch1
    protected void generateBranch(FileStoreTable table) throws Exception {
        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(0, 0, 0L));
            commit.commit(0, write.prepareCommit(false, 1));
        }

        assertThat(
                        getResult(
                                table.newRead(),
                                toSplits(table.newSnapshotReader().read().dataSplits()),
                                BATCH_ROW_TO_STRING))
                .containsExactlyInAnyOrder("0|0|0|binary|varbinary|mapKey:mapVal|multiset");

        table.createTag("tag1", 1);
        table.createBranch(BRANCH_NAME, "tag1");

        // verify that branch1 file exist
        BranchManager branchManager = table.branchManager();
        assertThat(branchManager.branchExists(BRANCH_NAME)).isTrue();

        // Verify branch1 and the main branch have the same data
        FileStoreTable tableBranch = createBranchTable(BRANCH_NAME);
        assertThat(
                        getResult(
                                tableBranch.newRead(),
                                toSplits(tableBranch.newSnapshotReader().read().dataSplits()),
                                BATCH_ROW_TO_STRING))
                .containsExactlyInAnyOrder("0|0|0|binary|varbinary|mapKey:mapVal|multiset");
    }

    protected void assertRollbackTo(
            FileStoreTable table,
            List<Long> expectedSnapshots,
            long expectedEarliest,
            long expectedLatest,
            List<String> expectedTags)
            throws IOException {
        SnapshotManager snapshotManager = table.snapshotManager();
        List<Long> snapshots = snapshotManager.snapshotIdStream().collect(Collectors.toList());
        assertThat(snapshots).containsExactlyInAnyOrderElementsOf(expectedSnapshots);
        assertThat(snapshotManager.earliestSnapshotId()).isEqualTo(expectedEarliest);
        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(expectedLatest);
        assertThat(table.tagManager().allTagNames())
                .containsExactlyInAnyOrderElementsOf(expectedTags);
    }
}
