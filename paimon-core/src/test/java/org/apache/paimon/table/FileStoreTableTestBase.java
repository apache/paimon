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

import org.apache.paimon.AbstractFileStore;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.TestFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.DataFormatTestUtil;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.mergetree.compact.ConcatRecordReader.ReaderSupplier;
import org.apache.paimon.operation.FileStoreTestUtils;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
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
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.BUCKET_KEY;
import static org.apache.paimon.CoreOptions.CHANGELOG_NUM_RETAINED_MAX;
import static org.apache.paimon.CoreOptions.CHANGELOG_NUM_RETAINED_MIN;
import static org.apache.paimon.CoreOptions.COMPACTION_MAX_FILE_NUM;
import static org.apache.paimon.CoreOptions.CONSUMER_IGNORE_PROGRESS;
import static org.apache.paimon.CoreOptions.ExpireExecutionMode;
import static org.apache.paimon.CoreOptions.FILE_FORMAT;
import static org.apache.paimon.CoreOptions.SNAPSHOT_EXPIRE_EXECUTION_MODE;
import static org.apache.paimon.CoreOptions.SNAPSHOT_EXPIRE_LIMIT;
import static org.apache.paimon.CoreOptions.SNAPSHOT_NUM_RETAINED_MAX;
import static org.apache.paimon.CoreOptions.SNAPSHOT_NUM_RETAINED_MIN;
import static org.apache.paimon.CoreOptions.WRITE_ONLY;
import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/** Base test class for {@link FileStoreTable}. */
public abstract class FileStoreTableTestBase {

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

    // for overwrite test
    protected static final RowType OVERWRITE_TEST_ROW_TYPE =
            RowType.of(
                    new DataType[] {
                        DataTypes.INT(), DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()
                    },
                    new String[] {"pk", "pt0", "pt1", "v"});

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
    protected String commitUser;

    @BeforeEach
    public void before() {
        tablePath = new Path(TraceableFileIO.SCHEME + "://" + tempDir.toString());
        commitUser = UUID.randomUUID().toString();
    }

    @AfterEach
    public void after() throws IOException {
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

    @ParameterizedTest(name = "dynamic = {0}, partition={2}")
    @MethodSource("overwriteTestData")
    public void testOverwriteNothing(
            boolean dynamicPartitionOverwrite,
            List<InternalRow> overwriteData,
            Map<String, String> overwritePartition,
            List<String> expected)
            throws Exception {
        FileStoreTable table = overwriteTestFileStoreTable();
        if (!dynamicPartitionOverwrite) {
            table =
                    table.copy(
                            Collections.singletonMap(
                                    CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key(), "false"));
        }

        // prepare data
        // (1, 1, 'A', 'Hi'), (2, 1, 'A', 'Hello'), (3, 1, 'A', 'World'),
        // (4, 1, 'B', 'To'), (5, 1, 'B', 'Apache'), (6, 1, 'B', 'Paimon')
        // (7, 2, 'A', 'Test')
        // (8, 2, 'B', 'Case')
        try (StreamTableWrite write = table.newWrite(commitUser);
                InnerTableCommit commit = table.newCommit(commitUser)) {
            write.write(overwriteRow(1, 1, "A", "Hi"));
            write.write(overwriteRow(2, 1, "A", "Hello"));
            write.write(overwriteRow(3, 1, "A", "World"));
            write.write(overwriteRow(4, 1, "B", "To"));
            write.write(overwriteRow(5, 1, "B", "Apache"));
            write.write(overwriteRow(6, 1, "B", "Paimon"));
            write.write(overwriteRow(7, 2, "A", "Test"));
            write.write(overwriteRow(8, 2, "B", "Case"));
            commit.commit(0, write.prepareCommit(true, 0));
        }

        // overwrite data
        try (StreamTableWrite write = table.newWrite(commitUser).withIgnorePreviousFiles(true);
                InnerTableCommit commit = table.newCommit(commitUser)) {
            for (InternalRow row : overwriteData) {
                write.write(row);
            }
            commit.withOverwrite(overwritePartition).commit(1, write.prepareCommit(true, 1));
        }

        // validate
        List<Split> splits = toSplits(table.newSnapshotReader().read().dataSplits());
        TableRead read = table.newRead();
        assertThat(
                        getResult(
                                read,
                                splits,
                                row ->
                                        DataFormatTestUtil.toStringNoRowKind(
                                                row, OVERWRITE_TEST_ROW_TYPE)))
                .hasSameElementsAs(expected);
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
                        Collections.singletonList(
                                "1|10|100|binary|varbinary|mapKey:mapVal|multiset"));
        assertThat(getResult(read, splits, binaryRow(2), 0, BATCH_ROW_TO_STRING))
                .hasSameElementsAs(
                        Collections.singletonList(
                                "2|21|201|binary|varbinary|mapKey:mapVal|multiset"));
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
                toSplits(
                        table.newSnapshotReader()
                                .withFilter(new PredicateBuilder(ROW_TYPE).equal(1, 5))
                                .read()
                                .dataSplits());
        assertThat(splits.size()).isEqualTo(1);
        assertThat(((DataSplit) splits.get(0)).bucket()).isEqualTo(1);
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
                LocalFileIO.create().listStatus(new Path(tablePath + "/pt=1/bucket-0"));
        assertThat(files).isEmpty();
        write.close();
        commit.close();
    }

    @Test
    public void testReadFilter() throws Exception {
        FileStoreTable table = createFileStoreTable();
        if (table.coreOptions().fileFormat().getFormatIdentifier().equals("parquet")) {
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
    public void testManifestCache() throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        conf ->
                                conf.set(
                                        CoreOptions.WRITE_MANIFEST_CACHE,
                                        MemorySize.ofMebiBytes(1)));
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        // lots of commits, produce lots of manifest
        List<String> expected = new ArrayList<>();
        int cnt = 50;
        for (int i = 0; i < cnt; i++) {
            write.write(rowData(i, i, (long) i));
            commit.commit(i, write.prepareCommit(false, i));
            expected.add(
                    String.format("%s|%s|%s|binary|varbinary|mapKey:mapVal|multiset", i, i, i));
        }
        write.close();

        // create new write and reload manifests
        write = table.newWrite(commitUser);
        for (int i = 0; i < cnt; i++) {
            write.write(rowData(i, i + 1, (long) i + 1));
            expected.add(
                    String.format(
                            "%s|%s|%s|binary|varbinary|mapKey:mapVal|multiset", i, i + 1, i + 1));
        }
        commit.commit(cnt, write.prepareCommit(false, cnt));
        commit.close();

        // check result
        List<String> result =
                getResult(table.newRead(), table.newScan().plan().splits(), BATCH_ROW_TO_STRING);
        assertThat(result.size()).isEqualTo(expected.size());
        assertThat(result).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testWriteWithoutCompactionAndExpiration() throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        conf -> {
                            conf.set(WRITE_ONLY, true);
                            conf.set(COMPACTION_MAX_FILE_NUM, 5);
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

        SnapshotManager snapshotManager =
                new SnapshotManager(FileIOFinder.find(tablePath), table.location());
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
        assertThatThrownBy(finalScan::plan)
                .satisfies(
                        anyCauseMatches(
                                OutOfRangeException.class, "The snapshot with id 5 has expired."));

        write.close();
        commit.close();
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

        List<java.nio.file.Path> files =
                Files.walk(new File(tablePath.toUri().getPath()).toPath())
                        .collect(Collectors.toList());
        assertThat(files.size()).isEqualTo(15);
        // table-path
        // table-path/snapshot
        // table-path/snapshot/LATEST
        // table-path/snapshot/EARLIEST
        // table-path/snapshot/snapshot-1
        // table-path/pt=0
        // table-path/pt=0/bucket-0
        // table-path/pt=0/bucket-0/data-0.orc
        // table-path/manifest
        // table-path/manifest/manifest-list-1
        // table-path/manifest/manifest-0
        // table-path/manifest/manifest-list-0
        // table-path/schema
        // table-path/schema/schema-0
        // table-path/tag
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

        List<java.nio.file.Path> files =
                Files.walk(new File(tablePath.toUri().getPath()).toPath())
                        .collect(Collectors.toList());
        assertThat(files.size()).isEqualTo(16);
        // case 0 plus 1:
        // table-path/tag/tag-test3
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

        List<java.nio.file.Path> files =
                Files.walk(new File(tablePath.toUri().getPath()).toPath())
                        .collect(Collectors.toList());
        assertThat(files.size()).isEqualTo(23);
        // case 0 plus 7:
        // table-path/manifest/manifest-list-2
        // table-path/manifest/manifest-list-3
        // table-path/manifest/manifest-1
        // table-path/snapshot/snapshot-2
        // table-path/pt=1
        // table-path/pt=1/bucket-0
        // table-path/pt=1/bucket-0/data-0.orc
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

        List<java.nio.file.Path> files =
                Files.walk(new File(tablePath.toUri().getPath()).toPath())
                        .collect(Collectors.toList());
        assertThat(files.size()).isEqualTo(16);
        // rollback snapshot case 0 plus 1:
        // table-path/tag/tag-test1
    }

    private FileStoreTable prepareRollbackTable(int commitTimes) throws Exception {
        FileStoreTable table = createFileStoreTable();
        prepareRollbackTable(commitTimes, table);
        return table;
    }

    protected FileStoreTable prepareRollbackTable(int commitTimes, FileStoreTable table)
            throws Exception {
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
        TagManager tagManager = new TagManager(new TraceableFileIO(), tablePath);
        assertThat(tagManager.tagExists("test-tag")).isTrue();

        // verify that test-tag is equal to snapshot 2
        Snapshot tagged = tagManager.taggedSnapshot("test-tag");
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
            TagManager tagManager = new TagManager(new TraceableFileIO(), tablePath);
            assertThat(tagManager.tagExists("test-tag")).isTrue();
            // verify that test-tag is equal to snapshot 1
            Snapshot tagged = tagManager.taggedSnapshot("test-tag");
            Snapshot snapshot1 = table.snapshotManager().snapshot(1);
            assertThat(tagged.equals(snapshot1)).isTrue();
            // snapshot 2
            write.write(rowData(2, 20, 200L));
            commit.commit(1, write.prepareCommit(false, 2));
            SnapshotManager snapshotManager = new SnapshotManager(new TraceableFileIO(), tablePath);
            // The snapshot 1 is expired.
            assertThat(snapshotManager.snapshotExists(1)).isFalse();
            table.createTag("test-tag-2", 1);
            // verify that tag file exist
            assertThat(tagManager.tagExists("test-tag-2")).isTrue();
            // verify that test-tag is equal to snapshot 1
            Snapshot tag2 = tagManager.taggedSnapshot("test-tag-2");
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
            TagManager tagManager = new TagManager(new TraceableFileIO(), tablePath);
            table.createTag("test-tag", 1);
            // verify that tag file exist
            assertThat(tagManager.tagExists("test-tag")).isTrue();
            // Create again
            table.createTag("test-tag", 1);
            Assertions.assertThatThrownBy(() -> table.createTag("test-tag", 2))
                    .hasMessageContaining("Tag name 'test-tag' already exists.");
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
        TagManager tagManager = new TagManager(new TraceableFileIO(), tablePath);
        assertThat(tagManager.tagExists("test-tag")).isTrue();

        // verify that test-tag is equal to snapshot 2
        Snapshot tagged = tagManager.taggedSnapshot("test-tag");
        Snapshot snapshot2 = table.snapshotManager().snapshot(2);
        assertThat(tagged.equals(snapshot2)).isTrue();

        table.createBranch("test-branch", "test-tag");

        // verify that branch file exist
        BranchManager branchManager = table.branchManager();
        assertThat(branchManager.branchExists("test-branch")).isTrue();

        // verify test-tag in test-branch is equal to snapshot 2
        Snapshot branchTag =
                Snapshot.fromPath(
                        new TraceableFileIO(), tagManager.branchTagPath("test-branch", "test-tag"));
        assertThat(branchTag.equals(snapshot2)).isTrue();

        // verify snapshot in test-branch is equal to snapshot 2
        SnapshotManager snapshotManager = new SnapshotManager(new TraceableFileIO(), tablePath);
        Snapshot branchSnapshot =
                Snapshot.fromPath(
                        new TraceableFileIO(),
                        snapshotManager.branchSnapshotPath("test-branch", 2));
        assertThat(branchSnapshot.equals(snapshot2)).isTrue();

        // verify schema in test-branch is equal to schema 0
        SchemaManager schemaManager = new SchemaManager(new TraceableFileIO(), tablePath);
        TableSchema branchSchema =
                SchemaManager.fromPath(
                        new TraceableFileIO(), schemaManager.branchSchemaPath("test-branch", 0));
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
                                IllegalArgumentException.class, "Tag name 'tag1' not exists."));

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
                                IllegalArgumentException.class,
                                String.format("Tag name '%s' is blank", "")));
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

        assertThatThrownBy(() -> table.deleteTag("tag1"))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class, "Tag 'tag1' doesn't exist."));
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
        ExecutorService executor = commit.getExpireMainExecutor();
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

        AbstractFileStore<?> store = (AbstractFileStore<?>) table.store();
        Set<Path> filesInUse =
                TestFileStore.getFilesInUse(
                        latestSnapshotId,
                        snapshotManager,
                        store.newScan(),
                        table.fileIO(),
                        store.pathFactory(),
                        store.manifestListFactory().create());

        List<Path> unusedFileList =
                Files.walk(Paths.get(tempDir.toString()))
                        .filter(Files::isRegularFile)
                        .filter(p -> !p.getFileName().toString().startsWith("snapshot"))
                        .filter(p -> !p.getFileName().toString().startsWith("schema"))
                        .filter(p -> !p.getFileName().toString().equals(SnapshotManager.LATEST))
                        .filter(p -> !p.getFileName().toString().equals(SnapshotManager.EARLIEST))
                        .map(p -> new Path(TraceableFileIO.SCHEME + "://" + p.toString()))
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
    public void testSchemaPathOption() throws Exception {
        String fakePath = "fake path";
        FileStoreTable table = createFileStoreTable(conf -> conf.set(CoreOptions.PATH, fakePath));
        String originSchemaPath = table.schema().options().get(CoreOptions.PATH.key());
        assertThat(originSchemaPath).isEqualTo(fakePath);
        // reset PATH of schema option to table location
        table = table.copy(Collections.emptyMap());
        String schemaPath = table.schema().options().get(CoreOptions.PATH.key());
        String tablePath = table.location().toString();
        assertThat(schemaPath).isEqualTo(tablePath);
    }

    @Test
    public void testBranchWriteAndRead() throws Exception {
        FileStoreTable table = createFileStoreTable();

        generateBranch(table);

        // Write data to branch1
        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser, BRANCH_NAME)) {
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
                                table.newRead(),
                                toSplits(table.newSnapshotReader(BRANCH_NAME).read().dataSplits()),
                                BATCH_ROW_TO_STRING))
                .containsExactlyInAnyOrder(
                        "0|0|0|binary|varbinary|mapKey:mapVal|multiset",
                        "2|20|200|binary|varbinary|mapKey:mapVal|multiset");

        // Write two rows data to branch1 again
        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser, BRANCH_NAME)) {
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
                                table.newRead(),
                                toSplits(table.newSnapshotReader(BRANCH_NAME).read().dataSplits()),
                                BATCH_ROW_TO_STRING))
                .containsExactlyInAnyOrder(
                        "0|0|0|binary|varbinary|mapKey:mapVal|multiset",
                        "2|20|200|binary|varbinary|mapKey:mapVal|multiset",
                        "3|30|300|binary|varbinary|mapKey:mapVal|multiset",
                        "4|40|400|binary|varbinary|mapKey:mapVal|multiset");
    }

    protected List<String> getResult(
            TableRead read,
            List<Split> splits,
            BinaryRow partition,
            int bucket,
            Function<InternalRow, String> rowDataToString)
            throws Exception {
        return getResult(read, getSplitsFor(splits, partition, bucket), rowDataToString);
    }

    protected List<String> getResult(
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

    private List<Split> getSplitsFor(List<Split> splits, BinaryRow partition, int bucket) {
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

    protected FileStoreTable createFileStoreTable() throws Exception {
        return createFileStoreTable(1);
    }

    protected abstract FileStoreTable createFileStoreTable(Consumer<Options> configure)
            throws Exception;

    protected abstract FileStoreTable overwriteTestFileStoreTable() throws Exception;

    private static InternalRow overwriteRow(Object... values) {
        return GenericRow.of(
                values[0],
                values[1],
                BinaryString.fromString((String) values[2]),
                BinaryString.fromString((String) values[3]));
    }

    private static List<Arguments> overwriteTestData() {
        // dynamic, overwrite data, overwrite partition, expected
        return Arrays.asList(
                // nothing happen
                arguments(
                        true,
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        Arrays.asList(
                                "1, 1, A, Hi",
                                "2, 1, A, Hello",
                                "3, 1, A, World",
                                "4, 1, B, To",
                                "5, 1, B, Apache",
                                "6, 1, B, Paimon",
                                "7, 2, A, Test",
                                "8, 2, B, Case")),
                // delete all data
                arguments(
                        false,
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        Collections.emptyList()),
                // specify one partition key
                arguments(
                        true,
                        Arrays.asList(
                                overwriteRow(1, 1, "A", "Where"), overwriteRow(2, 1, "A", "When")),
                        Collections.singletonMap("pt0", "1"),
                        Arrays.asList(
                                "1, 1, A, Where",
                                "2, 1, A, When",
                                "4, 1, B, To",
                                "5, 1, B, Apache",
                                "6, 1, B, Paimon",
                                "7, 2, A, Test",
                                "8, 2, B, Case")),
                arguments(
                        false,
                        Arrays.asList(
                                overwriteRow(1, 1, "A", "Where"), overwriteRow(2, 1, "A", "When")),
                        Collections.singletonMap("pt0", "1"),
                        Arrays.asList(
                                "1, 1, A, Where",
                                "2, 1, A, When",
                                "7, 2, A, Test",
                                "8, 2, B, Case")),
                // all dynamic
                arguments(
                        true,
                        Arrays.asList(
                                overwriteRow(4, 1, "B", "Where"),
                                overwriteRow(5, 1, "B", "When"),
                                overwriteRow(10, 2, "A", "Static"),
                                overwriteRow(11, 2, "A", "Dynamic")),
                        Collections.emptyMap(),
                        Arrays.asList(
                                "1, 1, A, Hi",
                                "2, 1, A, Hello",
                                "3, 1, A, World",
                                "4, 1, B, Where",
                                "5, 1, B, When",
                                "10, 2, A, Static",
                                "11, 2, A, Dynamic",
                                "8, 2, B, Case")),
                arguments(
                        false,
                        Arrays.asList(
                                overwriteRow(4, 1, "B", "Where"),
                                overwriteRow(5, 1, "B", "When"),
                                overwriteRow(10, 2, "A", "Static"),
                                overwriteRow(11, 2, "A", "Dynamic")),
                        Collections.emptyMap(),
                        Arrays.asList(
                                "4, 1, B, Where",
                                "5, 1, B, When",
                                "10, 2, A, Static",
                                "11, 2, A, Dynamic")));
    }

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
        TraceableFileIO fileIO = new TraceableFileIO();
        BranchManager branchManager = table.branchManager();
        assertThat(branchManager.branchExists(BRANCH_NAME)).isTrue();

        // Verify branch1 and the main branch have the same data
        assertThat(
                        getResult(
                                table.newRead(),
                                toSplits(table.newSnapshotReader(BRANCH_NAME).read().dataSplits()),
                                BATCH_ROW_TO_STRING))
                .containsExactlyInAnyOrder("0|0|0|binary|varbinary|mapKey:mapVal|multiset");
    }
}
