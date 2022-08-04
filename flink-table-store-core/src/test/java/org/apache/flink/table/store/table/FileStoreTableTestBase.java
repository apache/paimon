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

package org.apache.flink.table.store.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.mergetree.compact.ConcatRecordReader;
import org.apache.flink.table.store.file.mergetree.compact.ConcatRecordReader.ReaderSupplier;
import org.apache.flink.table.store.file.predicate.PredicateBuilder;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.file.utils.RecordReaderIterator;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.file.utils.TestAtomicRenameFileSystem;
import org.apache.flink.table.store.table.sink.FileCommittable;
import org.apache.flink.table.store.table.sink.TableCommit;
import org.apache.flink.table.store.table.sink.TableWrite;
import org.apache.flink.table.store.table.source.Split;
import org.apache.flink.table.store.table.source.TableRead;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.table.store.CoreOptions.BUCKET;
import static org.apache.flink.table.store.CoreOptions.BUCKET_KEY;
import static org.apache.flink.table.store.CoreOptions.COMPACTION_MAX_FILE_NUM;
import static org.apache.flink.table.store.CoreOptions.WRITE_SKIP_COMPACTION;
import static org.assertj.core.api.Assertions.assertThat;

/** Base test class for {@link FileStoreTable}. */
public abstract class FileStoreTableTestBase {

    protected static final RowType ROW_TYPE =
            RowType.of(
                    new LogicalType[] {
                        DataTypes.INT().getLogicalType(),
                        DataTypes.INT().getLogicalType(),
                        DataTypes.BIGINT().getLogicalType()
                    },
                    new String[] {"pt", "a", "b"});
    protected static final int[] PROJECTION = new int[] {2, 1};
    protected static final Function<RowData, String> BATCH_ROW_TO_STRING =
            rowData -> rowData.getInt(0) + "|" + rowData.getInt(1) + "|" + rowData.getLong(2);
    protected static final Function<RowData, String> BATCH_PROJECTED_ROW_TO_STRING =
            rowData -> rowData.getLong(0) + "|" + rowData.getInt(1);
    protected static final Function<RowData, String> STREAMING_ROW_TO_STRING =
            rowData ->
                    (rowData.getRowKind() == RowKind.INSERT ? "+" : "-")
                            + BATCH_ROW_TO_STRING.apply(rowData);
    protected static final Function<RowData, String> STREAMING_PROJECTED_ROW_TO_STRING =
            rowData ->
                    (rowData.getRowKind() == RowKind.INSERT ? "+" : "-")
                            + BATCH_PROJECTED_ROW_TO_STRING.apply(rowData);
    protected static final Function<RowData, String> CHANGELOG_ROW_TO_STRING =
            rowData ->
                    rowData.getRowKind().shortString() + " " + BATCH_ROW_TO_STRING.apply(rowData);

    @TempDir java.nio.file.Path tempDir;

    protected Path tablePath;

    @BeforeEach
    public void before() {
        tablePath = new Path(TestAtomicRenameFileSystem.SCHEME + "://" + tempDir.toString());
    }

    @Test
    public void testOverwrite() throws Exception {
        FileStoreTable table = createFileStoreTable();

        TableWrite write = table.newWrite();
        TableCommit commit = table.newCommit("user");
        write.write(GenericRowData.of(1, 10, 100L));
        write.write(GenericRowData.of(2, 20, 200L));
        commit.commit("0", write.prepareCommit(true));
        write.close();

        write = table.newWrite().withOverwrite(true);
        commit = table.newCommit("user");
        write.write(GenericRowData.of(2, 21, 201L));
        Map<String, String> overwritePartition = new HashMap<>();
        overwritePartition.put("pt", "2");
        commit.withOverwritePartition(overwritePartition).commit("1", write.prepareCommit(true));
        write.close();

        List<Split> splits = table.newScan().plan().splits;
        TableRead read = table.newRead();
        assertThat(getResult(read, splits, binaryRow(1), 0, BATCH_ROW_TO_STRING))
                .hasSameElementsAs(Collections.singletonList("1|10|100"));
        assertThat(getResult(read, splits, binaryRow(2), 0, BATCH_ROW_TO_STRING))
                .hasSameElementsAs(Collections.singletonList("2|21|201"));
    }

    @Test
    public void testBucketFilter() throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        conf -> {
                            conf.set(BUCKET, 5);
                            conf.set(BUCKET_KEY, "a");
                        });

        TableWrite write = table.newWrite();
        write.write(GenericRowData.of(1, 1, 2L));
        write.write(GenericRowData.of(1, 3, 4L));
        write.write(GenericRowData.of(1, 5, 6L));
        write.write(GenericRowData.of(1, 7, 8L));
        write.write(GenericRowData.of(1, 9, 10L));
        table.newCommit("user").commit("0", write.prepareCommit(true));
        write.close();

        List<Split> splits =
                table.newScan()
                        .withFilter(new PredicateBuilder(ROW_TYPE).equal(1, 5))
                        .plan()
                        .splits;
        assertThat(splits.size()).isEqualTo(1);
        assertThat(splits.get(0).bucket()).isEqualTo(1);
    }

    @Test
    public void testReadFilter() throws Exception {
        FileStoreTable table = createFileStoreTable();

        TableWrite write = table.newWrite();
        TableCommit commit = table.newCommit("user");

        write.write(GenericRowData.of(1, 10, 100L));
        write.write(GenericRowData.of(1, 20, 200L));
        commit.commit("0", write.prepareCommit(true));

        write.write(GenericRowData.of(1, 30, 300L));
        write.write(GenericRowData.of(1, 40, 400L));
        commit.commit("1", write.prepareCommit(true));

        write.write(GenericRowData.of(1, 50, 500L));
        write.write(GenericRowData.of(1, 60, 600L));
        commit.commit("2", write.prepareCommit(true));

        write.close();

        PredicateBuilder builder = new PredicateBuilder(ROW_TYPE);
        List<Split> splits = table.newScan().plan().splits;
        TableRead read = table.newRead().withFilter(builder.equal(2, 300L));
        assertThat(getResult(read, splits, binaryRow(1), 0, BATCH_ROW_TO_STRING))
                .hasSameElementsAs(Arrays.asList("1|30|300", "1|40|400"));
    }

    @Test
    public void testPartitionEmptyWriter() throws Exception {
        FileStoreTable table = createFileStoreTable();
        TableWrite write = table.newWrite();
        TableCommit commit = table.newCommit("user");

        for (int i = 0; i < 4; i++) {
            // write lots of records, let compaction be slower
            for (int j = 0; j < 1000; j++) {
                write.write(GenericRowData.of(1, 10 * i * j, 100L * i * j));
            }
            commit.commit(String.valueOf(i), write.prepareCommit(false));
        }

        write.write(GenericRowData.of(1, 40, 400L));
        List<FileCommittable> commit4 = write.prepareCommit(false);
        // trigger compaction, but not wait it.

        write.write(GenericRowData.of(2, 20, 200L));
        List<FileCommittable> commit5 = write.prepareCommit(true);
        // wait compaction finish
        // commit5 should be a compaction commit

        write.write(GenericRowData.of(1, 60, 600L));
        List<FileCommittable> commit6 = write.prepareCommit(true);
        // if remove writer too fast, will see old files, do another compaction
        // then will be conflicts

        commit.commit("4", commit4);
        commit.commit("5", commit5);
        commit.commit("6", commit6);

        write.close();
    }

    @Test
    public void testWriteNoCompaction() throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        conf -> {
                            conf.set(WRITE_SKIP_COMPACTION, true);
                            conf.set(COMPACTION_MAX_FILE_NUM, 5);
                        });

        TableWrite write = table.newWrite();
        TableCommit commit = table.newCommit("user");
        for (int i = 0; i < 10; i++) {
            write.write(GenericRowData.of(1, 1, 100L));
            commit.commit(String.valueOf(i), write.prepareCommit(true));
        }
        write.close();

        List<DataFileMeta> files =
                table.newScan().plan().splits.stream()
                        .flatMap(split -> split.files().stream())
                        .collect(Collectors.toList());
        for (DataFileMeta file : files) {
            assertThat(file.level()).isEqualTo(0);
        }

        SnapshotManager snapshotManager = new SnapshotManager(table.location());
        Long latestSnapshotId = snapshotManager.latestSnapshotId();
        assertThat(latestSnapshotId).isNotNull();
        for (int i = 1; i <= latestSnapshotId; i++) {
            Snapshot snapshot = snapshotManager.snapshot(i);
            assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);
        }
    }

    protected List<String> getResult(
            TableRead read,
            List<Split> splits,
            BinaryRowData partition,
            int bucket,
            Function<RowData, String> rowDataToString)
            throws Exception {
        List<ReaderSupplier<RowData>> readers = new ArrayList<>();
        for (Split split : getSplitsFor(splits, partition, bucket)) {
            readers.add(() -> read.createReader(split));
        }
        RecordReader<RowData> recordReader = ConcatRecordReader.create(readers);
        RecordReaderIterator<RowData> iterator = new RecordReaderIterator<>(recordReader);
        List<String> result = new ArrayList<>();
        while (iterator.hasNext()) {
            RowData rowData = iterator.next();
            result.add(rowDataToString.apply(rowData));
        }
        iterator.close();
        return result;
    }

    private List<Split> getSplitsFor(List<Split> splits, BinaryRowData partition, int bucket) {
        List<Split> result = new ArrayList<>();
        for (Split split : splits) {
            if (split.partition().equals(partition) && split.bucket() == bucket) {
                result.add(split);
            }
        }
        return result;
    }

    protected BinaryRowData binaryRow(int a) {
        BinaryRowData b = new BinaryRowData(1);
        BinaryRowWriter writer = new BinaryRowWriter(b);
        writer.writeInt(0, a);
        writer.complete();
        return b;
    }

    protected FileStoreTable createFileStoreTable(int numOfBucket) throws Exception {
        return createFileStoreTable(conf -> conf.set(BUCKET, numOfBucket));
    }

    protected FileStoreTable createFileStoreTable() throws Exception {
        return createFileStoreTable(1);
    }

    protected abstract FileStoreTable createFileStoreTable(Consumer<Configuration> configure)
            throws Exception;
}
