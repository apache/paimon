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
import org.apache.paimon.Snapshot;
import org.apache.paimon.append.AppendCompactTask;
import org.apache.paimon.bucket.DefaultBucketFunction;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.fileindex.bitmap.BitmapFileIndexFactory;
import org.apache.paimon.fileindex.bloomfilter.BloomFilterFileIndexFactory;
import org.apache.paimon.fileindex.bsi.BitSliceIndexBitmapFileIndexFactory;
import org.apache.paimon.fileindex.rangebitmap.RangeBitmapFileIndexFactory;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.operation.BaseAppendFileStoreWrite;
import org.apache.paimon.operation.FileStoreWrite;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BranchMergeHandler;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.RoaringBitmap32;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.BUCKET_APPEND_ORDERED;
import static org.apache.paimon.CoreOptions.BUCKET_KEY;
import static org.apache.paimon.CoreOptions.DATA_FILE_PATH_DIRECTORY;
import static org.apache.paimon.CoreOptions.FILE_FORMAT;
import static org.apache.paimon.CoreOptions.FILE_FORMAT_PARQUET;
import static org.apache.paimon.CoreOptions.FILE_INDEX_IN_MANIFEST_THRESHOLD;
import static org.apache.paimon.CoreOptions.METADATA_STATS_MODE;
import static org.apache.paimon.CoreOptions.SOURCE_SPLIT_TARGET_SIZE;
import static org.apache.paimon.CoreOptions.WRITE_ONLY;
import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.apache.paimon.predicate.SortValue.NullOrdering.NULLS_LAST;
import static org.apache.paimon.predicate.SortValue.SortDirection.DESCENDING;
import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link AppendOnlyFileStoreTable}. */
public class AppendOnlySimpleTableTest extends SimpleTableTestBase {

    @Test
    public void testOverwriteSameFiles() throws Exception {
        FileStoreTable table = createFileStoreTable();

        // write files
        List<CommitMessage> commitMessages;
        try (BatchTableWrite write = table.newBatchWriteBuilder().newWrite()) {
            write.write(rowData(1, 10, 100L));
            commitMessages = write.prepareCommit();
        }

        // first commit
        try (BatchTableCommit commit = table.newBatchWriteBuilder().withOverwrite().newCommit()) {
            commit.commit(commitMessages);
        }

        // second commit should throw exception
        try (BatchTableCommit commit = table.newBatchWriteBuilder().withOverwrite().newCommit()) {
            assertThatThrownBy(() -> commit.commit(commitMessages))
                    .hasMessageContaining("File deletion conflicts detected! Give up committing");
        }
    }

    @Test
    public void testBucketedAppendTableWriteWithInit() throws Exception {
        innerTestBucketedAppendTableWriteInit(true);
    }

    @Test
    public void testBucketedAppendTableWriteNoInit() throws Exception {
        innerTestBucketedAppendTableWriteInit(false);
    }

    public void innerTestBucketedAppendTableWriteInit(boolean ordered) throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        options -> {
                            options.set(BUCKET, 2);
                            options.set(BUCKET_KEY, "a");
                            options.set(WRITE_ONLY, true);
                            options.set(BUCKET_APPEND_ORDERED, ordered);
                        });

        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();

        // 1. first write
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(rowData(1, 10, 100L));
            commit.commit(write.prepareCommit());
        }

        // 2. delete all manifests
        ManifestList manifestList = table.store().manifestListFactory().create();
        ManifestFile manifestFile = table.store().manifestFileFactory().create();
        List<ManifestFileMeta> manifests =
                manifestList.readAllManifests(table.latestSnapshot().get());
        for (ManifestFileMeta manifest : manifests) {
            manifestFile.delete(manifest.fileName());
        }

        // 3. check new write
        try (BatchTableWrite write = writeBuilder.newWrite()) {
            if (ordered) {
                assertThatThrownBy(() -> write.write(rowData(1, 10, 100L)))
                        .hasMessageContaining("FileNotFoundException");
            } else {
                // no exception
                write.write(rowData(1, 10, 100L));
            }
        }
    }

    @Test
    public void testOverwriteNeverFail() throws Exception {
        FileStoreTable table = createFileStoreTable();

        Runnable writeRecord =
                () -> {
                    BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
                    try (BatchTableWrite write = writeBuilder.newWrite();
                            BatchTableCommit commit = writeBuilder.newCommit()) {
                        write.write(rowData(1, 10, 100L));
                        commit.commit(write.prepareCommit());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                };

        Runnable overwrite =
                () -> {
                    BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder().withOverwrite();
                    try (BatchTableWrite write = writeBuilder.newWrite();
                            BatchTableCommit commit = writeBuilder.newCommit()) {
                        write.write(rowData(1, 10, 100L));
                        commit.commit(write.prepareCommit());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                };

        Runnable compact =
                () -> {
                    BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder().withOverwrite();
                    try (BatchTableWrite write = writeBuilder.newWrite();
                            BatchTableCommit commit = writeBuilder.newCommit()) {
                        List<DataSplit> splits =
                                (List) table.newReadBuilder().newScan().plan().splits();
                        List<DataFileMeta> files =
                                splits.stream()
                                        .flatMap(s -> s.dataFiles().stream())
                                        .collect(Collectors.toList());
                        FileStoreWrite fileStoreWrite = ((TableWriteImpl) write).getWrite();
                        CommitMessage commitMessage =
                                new AppendCompactTask(splits.get(0).partition(), files)
                                        .doCompact(
                                                table, (BaseAppendFileStoreWrite) fileStoreWrite);
                        commit.commit(Collections.singletonList(commitMessage));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                };

        AtomicReference<Exception> exception = new AtomicReference<>();

        Thread thread1 =
                new Thread(
                        () -> {
                            for (int i = 0; i < 10; i++) {
                                try {
                                    writeRecord.run();
                                    overwrite.run();
                                } catch (Exception e) {
                                    exception.set(e);
                                }
                            }
                        });

        Thread thread2 =
                new Thread(
                        () -> {
                            for (int i = 0; i < 10; i++) {
                                try {
                                    writeRecord.run();
                                    compact.run();
                                } catch (Exception ignored) {
                                }
                            }
                        });

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();

        assertThat(exception.get()).isNull();
    }

    @Test
    public void testDiscardDuplicateFiles() throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        options -> options.set(CoreOptions.COMMIT_DISCARD_DUPLICATE_FILES, true));
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        List<CommitMessage> commitMessages;
        try (BatchTableWrite write = writeBuilder.newWrite()) {
            write.write(rowData(1, 10, 100L));
            commitMessages = write.prepareCommit();
        }
        Runnable doCommit =
                () -> {
                    try (BatchTableCommit commit = writeBuilder.newCommit()) {
                        commit.commit(commitMessages);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                };

        doCommit.run();
        doCommit.run();
        List<Split> splits = table.newReadBuilder().newScan().plan().splits();
        assertThat(splits.size()).isEqualTo(1);
        assertThat(splits.get(0).convertToRawFiles()).map(List::size).get().isEqualTo(1);
    }

    @Test
    public void testDiscardDuplicateFilesMultiThread() throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        options -> {
                            options.set(CoreOptions.COMMIT_DISCARD_DUPLICATE_FILES, true);
                            options.set(CoreOptions.COMMIT_MAX_RETRIES, 50);
                            options.set(CoreOptions.COMMIT_MAX_RETRY_WAIT, Duration.ofMillis(100));
                            // Keep all snapshots so concurrent expiry does not race readers.
                            options.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN, 1000);
                            options.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX, 1000);
                        });
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        List<List<CommitMessage>> messages = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            try (BatchTableWrite write = writeBuilder.newWrite()) {
                write.write(rowData(1, 10, 100L));
                messages.add(write.prepareCommit());
            }
        }
        int commitThreadNum = 10;
        int commitsPerThread = 10;
        Runnable asserter =
                () -> {
                    List<Split> splits = table.newReadBuilder().newScan().plan().splits();
                    assertThat(splits.size()).isEqualTo(1);
                    assertThat(splits.get(0).convertToRawFiles().get().size())
                            .isLessThanOrEqualTo(messages.size());
                };

        ExecutorService pool = Executors.newFixedThreadPool(commitThreadNum);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int thread = 0; thread < commitThreadNum; thread++) {
                int threadId = thread;
                futures.add(
                        pool.submit(
                                () -> {
                                    for (int round = 0; round < commitsPerThread; round++) {
                                        int messageIndex = (threadId + round) % messages.size();
                                        try (BatchTableCommit commit = writeBuilder.newCommit()) {
                                            commit.commit(messages.get(messageIndex));
                                        } catch (Exception e) {
                                            throw new RuntimeException(
                                                    String.format(
                                                            "Failed to commit message %s in thread %s round %s.",
                                                            messageIndex, threadId, round),
                                                    e);
                                        }
                                    }
                                }));
            }
            for (Future<?> future : futures) {
                future.get();
            }
        } finally {
            pool.shutdownNow();
        }
        asserter.run();
    }

    @Test
    public void testDynamicBucketNoSelector() throws Exception {
        assertThat(
                        createFileStoreTable(options -> options.set("bucket", "-1"))
                                .newBatchWriteBuilder()
                                .newWriteSelector())
                .isEmpty();
    }

    @Test
    public void testMinMaxRowIdNull() throws Exception {
        writeData();
        FileStoreTable table = createFileStoreTable();
        List<ManifestFileMeta> manifests =
                table.store()
                        .manifestListFactory()
                        .create()
                        .readDataManifests(table.latestSnapshot().get());
        assertThat(manifests.size()).isGreaterThan(0);
        for (ManifestFileMeta manifest : manifests) {
            assertThat(manifest.minRowId()).isNull();
            assertThat(manifest.maxRowId()).isNull();
        }
    }

    @Test
    public void testReadDeletedFiles() throws Exception {
        writeData();
        FileStoreTable table = createFileStoreTable();
        List<Split> splits = toSplits(table.newSnapshotReader().read().dataSplits());
        TableRead read = table.newRead();

        // delete one file
        DataSplit split = (DataSplit) splits.get(0);
        Path path =
                table.store()
                        .pathFactory()
                        .createDataFilePathFactory(split.partition(), split.bucket())
                        .toPath(split.dataFiles().get(0));
        table.fileIO().deleteQuietly(path);

        // read
        assertThatThrownBy(() -> getResult(read, splits, BATCH_ROW_TO_STRING))
                .hasMessageContaining("snapshot expires too fast");
    }

    @Test
    public void testBatchReadWrite() throws Exception {
        writeData();
        FileStoreTable table = createFileStoreTable();

        List<Split> splits = toSplits(table.newSnapshotReader().read().dataSplits());
        TableRead read = table.newRead();
        assertThat(getResult(read, splits, binaryRow(1), 0, BATCH_ROW_TO_STRING))
                .hasSameElementsAs(
                        Arrays.asList(
                                "1|10|100|binary|varbinary|mapKey:mapVal|multiset",
                                "1|11|101|binary|varbinary|mapKey:mapVal|multiset",
                                "1|12|102|binary|varbinary|mapKey:mapVal|multiset",
                                "1|11|101|binary|varbinary|mapKey:mapVal|multiset",
                                "1|12|102|binary|varbinary|mapKey:mapVal|multiset"));
        assertThat(getResult(read, splits, binaryRow(2), 0, BATCH_ROW_TO_STRING))
                .hasSameElementsAs(
                        Arrays.asList(
                                "2|20|200|binary|varbinary|mapKey:mapVal|multiset",
                                "2|21|201|binary|varbinary|mapKey:mapVal|multiset",
                                "2|22|202|binary|varbinary|mapKey:mapVal|multiset",
                                "2|21|201|binary|varbinary|mapKey:mapVal|multiset"));
    }

    @Test
    public void testReadWriteWithDataDirectory() throws Exception {
        Consumer<Options> optionsSetter = options -> options.set(DATA_FILE_PATH_DIRECTORY, "data");
        writeData(optionsSetter);
        FileStoreTable table = createFileStoreTable(optionsSetter);

        assertThat(table.fileIO().exists(new Path(tablePath, "data/pt=1"))).isTrue();

        List<Split> splits = toSplits(table.newSnapshotReader().read().dataSplits());
        TableRead read = table.newRead();
        assertThat(getResult(read, splits, binaryRow(1), 0, BATCH_ROW_TO_STRING))
                .hasSameElementsAs(
                        Arrays.asList(
                                "1|10|100|binary|varbinary|mapKey:mapVal|multiset",
                                "1|11|101|binary|varbinary|mapKey:mapVal|multiset",
                                "1|12|102|binary|varbinary|mapKey:mapVal|multiset",
                                "1|11|101|binary|varbinary|mapKey:mapVal|multiset",
                                "1|12|102|binary|varbinary|mapKey:mapVal|multiset"));
    }

    @Test
    public void testBatchRecordsWrite() throws Exception {
        FileStoreTable table = createFileStoreTable();

        List<InternalRow> list = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            list.add(rowData(1, 10, 100L));
        }

        BatchTableWrite write = table.newBatchWriteBuilder().newWrite();

        write.writeBundle(
                binaryRow(1),
                0,
                new BundleRecords() {
                    @Override
                    public long rowCount() {
                        return 1000;
                    }

                    @Override
                    public Iterator<InternalRow> iterator() {
                        return list.iterator();
                    }
                });

        List<CommitMessage> commitMessages = write.prepareCommit();

        table.newBatchWriteBuilder().newCommit().commit(commitMessages);

        List<Split> splits = toSplits(table.newSnapshotReader().read().dataSplits());
        TableRead read = table.newRead();
        AtomicInteger i = new AtomicInteger(0);
        read.createReader(splits)
                .forEachRemaining(
                        r -> {
                            i.incrementAndGet();
                            assertThat(r.getInt(1)).isEqualTo(10);
                            assertThat(r.getLong(2)).isEqualTo(100);
                        });
        Assertions.assertThat(i.get()).isEqualTo(1000);
    }

    @Test
    public void testBranchBatchReadWrite() throws Exception {
        FileStoreTable table = createFileStoreTable();
        generateBranch(table);

        FileStoreTable tableBranch = createBranchTable(BRANCH_NAME);
        writeBranchData(tableBranch);
        List<Split> splits = toSplits(tableBranch.newSnapshotReader().read().dataSplits());
        TableRead read = tableBranch.newRead();
        assertThat(getResult(read, splits, binaryRow(1), 0, BATCH_ROW_TO_STRING))
                .hasSameElementsAs(
                        Arrays.asList(
                                "1|10|100|binary|varbinary|mapKey:mapVal|multiset",
                                "1|11|101|binary|varbinary|mapKey:mapVal|multiset",
                                "1|12|102|binary|varbinary|mapKey:mapVal|multiset",
                                "1|11|101|binary|varbinary|mapKey:mapVal|multiset",
                                "1|12|102|binary|varbinary|mapKey:mapVal|multiset"));
        assertThat(getResult(read, splits, binaryRow(2), 0, BATCH_ROW_TO_STRING))
                .hasSameElementsAs(
                        Arrays.asList(
                                "2|20|200|binary|varbinary|mapKey:mapVal|multiset",
                                "2|21|201|binary|varbinary|mapKey:mapVal|multiset",
                                "2|22|202|binary|varbinary|mapKey:mapVal|multiset",
                                "2|21|201|binary|varbinary|mapKey:mapVal|multiset"));
    }

    @Test
    public void testBatchProjection() throws Exception {
        writeData();
        FileStoreTable table = createFileStoreTable();

        List<Split> splits = toSplits(table.newSnapshotReader().read().dataSplits());
        TableRead read = table.newRead().withProjection(PROJECTION);
        assertThat(getResult(read, splits, binaryRow(1), 0, BATCH_PROJECTED_ROW_TO_STRING))
                .hasSameElementsAs(Arrays.asList("100|10", "101|11", "102|12", "101|11", "102|12"));
        assertThat(getResult(read, splits, binaryRow(2), 0, BATCH_PROJECTED_ROW_TO_STRING))
                .hasSameElementsAs(Arrays.asList("200|20", "201|21", "202|22", "201|21"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testBatchFilter(boolean statsDenseStore) throws Exception {
        Consumer<Options> optionsSetter =
                options -> {
                    if (statsDenseStore) {
                        options.set(CoreOptions.METADATA_STATS_MODE, "none");
                        options.set("fields.b.stats-mode", "full");
                    }
                };
        writeData(optionsSetter);
        FileStoreTable table = createFileStoreTable(optionsSetter);
        PredicateBuilder builder = new PredicateBuilder(table.schema().logicalRowType());

        Predicate predicate = builder.equal(2, 201L);
        List<Split> splits =
                toSplits(table.newSnapshotReader().withFilter(predicate).read().dataSplits());
        TableRead read = table.newRead();
        assertThat(getResult(read, splits, binaryRow(1), 0, BATCH_ROW_TO_STRING)).isEmpty();
        assertThat(getResult(read, splits, binaryRow(2), 0, BATCH_ROW_TO_STRING))
                .hasSameElementsAs(
                        Arrays.asList(
                                "2|21|201|binary|varbinary|mapKey:mapVal|multiset",
                                // this record is in the same file with the first "2|21|201"
                                "2|22|202|binary|varbinary|mapKey:mapVal|multiset",
                                "2|21|201|binary|varbinary|mapKey:mapVal|multiset"));
    }

    @Test
    public void testBatchFilterWithExecution() throws Exception {
        writeData();
        FileStoreTable table = createFileStoreTable();
        PredicateBuilder builder = new PredicateBuilder(table.schema().logicalRowType());

        List<Split> splits = toSplits(table.newSnapshotReader().read().dataSplits());

        // simple
        TableRead read = table.newRead().withFilter(builder.equal(2, 201L)).executeFilter();
        assertThat(getResult(read, splits, binaryRow(1), 0, BATCH_ROW_TO_STRING)).isEmpty();
        assertThat(getResult(read, splits, binaryRow(2), 0, BATCH_ROW_TO_STRING))
                .hasSameElementsAs(
                        Arrays.asList(
                                "2|21|201|binary|varbinary|mapKey:mapVal|multiset",
                                "2|21|201|binary|varbinary|mapKey:mapVal|multiset"));

        // or
        read =
                table.newRead()
                        .withFilter(
                                PredicateBuilder.or(builder.equal(2, 201L), builder.equal(2, 500L)))
                        .executeFilter();
        assertThat(getResult(read, splits, binaryRow(1), 0, BATCH_ROW_TO_STRING)).isEmpty();
        assertThat(getResult(read, splits, binaryRow(2), 0, BATCH_ROW_TO_STRING))
                .hasSameElementsAs(
                        Arrays.asList(
                                "2|21|201|binary|varbinary|mapKey:mapVal|multiset",
                                "2|21|201|binary|varbinary|mapKey:mapVal|multiset"));

        // projection all in
        read =
                table.newRead()
                        .withFilter(
                                PredicateBuilder.or(builder.equal(2, 201L), builder.equal(2, 500L)))
                        .withProjection(new int[] {3, 2})
                        .executeFilter();
        Function<InternalRow, String> toString =
                rowData -> rowData.getLong(1) + "|" + new String(rowData.getBinary(0));
        assertThat(getResult(read, splits, binaryRow(1), 0, toString)).isEmpty();
        assertThat(getResult(read, splits, binaryRow(2), 0, toString))
                .hasSameElementsAs(Arrays.asList("201|binary", "201|binary"));

        // projection contains unknown index or
        read =
                table.newRead()
                        .withFilter(
                                PredicateBuilder.or(builder.equal(2, 201L), builder.equal(0, 1)))
                        .withProjection(new int[] {3, 2})
                        .executeFilter();
        assertThat(getResult(read, splits, binaryRow(2), 0, toString))
                .hasSameElementsAs(
                        Arrays.asList("200|binary", "201|binary", "202|binary", "201|binary"));

        // projection contains unknown index and
        read =
                table.newRead()
                        .withFilter(
                                PredicateBuilder.and(builder.equal(2, 201L), builder.equal(0, 1)))
                        .withProjection(new int[] {3, 2})
                        .executeFilter();
        assertThat(getResult(read, splits, binaryRow(1), 0, toString)).isEmpty();
        assertThat(getResult(read, splits, binaryRow(2), 0, toString))
                .hasSameElementsAs(Arrays.asList("201|binary", "201|binary"));
    }

    @Test
    public void testSplitOrder() throws Exception {
        FileStoreTable table = createFileStoreTable();

        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        write.write(rowData(1, 10, 100L));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData(2, 22, 202L));
        commit.commit(1, write.prepareCommit(true, 1));

        write.write(rowData(3, 33, 303L));
        commit.commit(2, write.prepareCommit(true, 2));
        write.close();
        commit.close();

        List<Split> splits = toSplits(table.newSnapshotReader().read().dataSplits());
        int[] partitions =
                splits.stream()
                        .map(split -> ((DataSplit) split).partition().getInt(0))
                        .mapToInt(Integer::intValue)
                        .toArray();
        assertThat(partitions).containsExactly(1, 2, 3);
    }

    @Test
    public void testBatchSplitOrderByPartition() throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        options -> options.set(CoreOptions.SCAN_PLAN_SORT_PARTITION, true));

        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        write.write(rowData(3, 33, 303L));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData(1, 10, 100L));
        commit.commit(1, write.prepareCommit(true, 1));

        write.write(rowData(2, 22, 202L));
        commit.commit(2, write.prepareCommit(true, 2));

        write.close();
        commit.close();

        List<Split> splits = toSplits(table.newSnapshotReader().read().dataSplits());
        int[] partitions =
                splits.stream()
                        .map(split -> ((DataSplit) split).partition().getInt(0))
                        .mapToInt(Integer::intValue)
                        .toArray();
        assertThat(partitions).containsExactly(1, 2, 3);
    }

    @Test
    public void testBranchStreamingReadWrite() throws Exception {
        FileStoreTable table = createFileStoreTable();
        generateBranch(table);

        FileStoreTable tableBranch = createBranchTable(BRANCH_NAME);
        writeBranchData(tableBranch);

        List<Split> splits =
                toSplits(
                        tableBranch
                                .newSnapshotReader()
                                .withMode(ScanMode.DELTA)
                                .read()
                                .dataSplits());
        TableRead read = tableBranch.newRead();

        assertThat(getResult(read, splits, binaryRow(1), 0, STREAMING_ROW_TO_STRING))
                .isEqualTo(
                        Arrays.asList(
                                "+1|11|101|binary|varbinary|mapKey:mapVal|multiset",
                                "+1|12|102|binary|varbinary|mapKey:mapVal|multiset"));
        assertThat(getResult(read, splits, binaryRow(2), 0, STREAMING_ROW_TO_STRING))
                .isEqualTo(
                        Collections.singletonList(
                                "+2|21|201|binary|varbinary|mapKey:mapVal|multiset"));
    }

    @Test
    public void testStreamingSplitInUnawareBucketMode() throws Exception {
        // in unaware-bucket mode, we split files into splits all the time
        FileStoreTable table =
                createUnawareBucketFileStoreTable(
                        options -> options.set(CoreOptions.SOURCE_SPLIT_TARGET_SIZE.key(), "1 M"));

        StreamTableScan scan = table.newStreamScan();

        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);
        List<CommitMessage> result = new ArrayList<>();
        write.write(rowData(3, 33, 303L));
        result.addAll(write.prepareCommit(true, 0));
        write.write(rowData(1, 10, 100L));
        result.addAll(write.prepareCommit(true, 0));
        write.write(rowData(2, 22, 202L));
        result.addAll(write.prepareCommit(true, 0));
        commit.commit(0, result);
        result.clear();
        assertThat(scan.plan().splits().size()).isEqualTo(3);

        write.write(rowData(3, 33, 303L));
        result.addAll(write.prepareCommit(true, 1));
        write.write(rowData(1, 10, 100L));
        result.addAll(write.prepareCommit(true, 1));
        write.write(rowData(2, 22, 202L));
        result.addAll(write.prepareCommit(true, 1));
        commit.commit(1, result);
        assertThat(scan.plan().splits().size()).isEqualTo(3);

        write.close();
        commit.close();
    }

    @Test
    public void testBloomFilterInMemory() throws Exception {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("index_column", DataTypes.STRING())
                        .field("index_column2", DataTypes.INT())
                        .field("index_column3", DataTypes.BIGINT())
                        .build();
        // in unaware-bucket mode, we split files into splits all the time
        FileStoreTable table =
                createUnawareBucketFileStoreTable(
                        rowType,
                        options -> {
                            options.set(
                                    FileIndexOptions.FILE_INDEX
                                            + "."
                                            + BloomFilterFileIndexFactory.BLOOM_FILTER
                                            + "."
                                            + CoreOptions.COLUMNS,
                                    "index_column, index_column2, index_column3");
                            options.set(
                                    FileIndexOptions.FILE_INDEX
                                            + "."
                                            + BloomFilterFileIndexFactory.BLOOM_FILTER
                                            + ".index_column.items",
                                    "150");
                            options.set(
                                    FileIndexOptions.FILE_INDEX
                                            + "."
                                            + BloomFilterFileIndexFactory.BLOOM_FILTER
                                            + ".index_column2.items",
                                    "150");
                            options.set(
                                    FileIndexOptions.FILE_INDEX
                                            + "."
                                            + BloomFilterFileIndexFactory.BLOOM_FILTER
                                            + ".index_column3.items",
                                    "150");
                            options.set(FILE_INDEX_IN_MANIFEST_THRESHOLD.key(), "500 B");
                        });

        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);
        List<CommitMessage> result = new ArrayList<>();
        write.write(GenericRow.of(1, BinaryString.fromString("a"), 2, 3L));
        write.write(GenericRow.of(1, BinaryString.fromString("c"), 2, 3L));
        result.addAll(write.prepareCommit(true, 0));
        write.write(GenericRow.of(1, BinaryString.fromString("b"), 2, 3L));
        result.addAll(write.prepareCommit(true, 0));
        commit.commit(0, result);
        result.clear();

        TableScan.Plan plan =
                table.newScan()
                        .withFilter(
                                new PredicateBuilder(rowType)
                                        .equal(1, BinaryString.fromString("b")))
                        .plan();
        List<DataFileMeta> metas =
                plan.splits().stream()
                        .flatMap(split -> ((DataSplit) split).dataFiles().stream())
                        .collect(Collectors.toList());
        assertThat(metas.size()).isEqualTo(1);
    }

    @Test
    public void testBloomFilterInDisk() throws Exception {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("index_column", DataTypes.STRING())
                        .field("index_column2", DataTypes.INT())
                        .field("index_column3", DataTypes.BIGINT())
                        .build();
        // in unaware-bucket mode, we split files into splits all the time
        FileStoreTable table =
                createUnawareBucketFileStoreTable(
                        rowType,
                        options -> {
                            options.set(
                                    FileIndexOptions.FILE_INDEX
                                            + "."
                                            + BloomFilterFileIndexFactory.BLOOM_FILTER
                                            + "."
                                            + CoreOptions.COLUMNS,
                                    "index_column, index_column2, index_column3");
                            options.set(FILE_INDEX_IN_MANIFEST_THRESHOLD.key(), "50 B");
                        });

        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);
        List<CommitMessage> result = new ArrayList<>();
        write.write(GenericRow.of(1, BinaryString.fromString("a"), 2, 3L));
        write.write(GenericRow.of(1, BinaryString.fromString("c"), 2, 3L));
        result.addAll(write.prepareCommit(true, 0));
        write.write(GenericRow.of(1, BinaryString.fromString("b"), 2, 3L));
        result.addAll(write.prepareCommit(true, 0));
        commit.commit(0, result);
        result.clear();

        TableScan.Plan plan =
                table.newScan()
                        .withFilter(
                                new PredicateBuilder(rowType)
                                        .equal(1, BinaryString.fromString("b")))
                        .plan();
        List<DataFileMeta> metas =
                plan.splits().stream()
                        .flatMap(split -> ((DataSplit) split).dataFiles().stream())
                        .collect(Collectors.toList());
        assertThat(metas.size()).isEqualTo(2);

        RecordReader<InternalRow> reader =
                table.newRead()
                        .withFilter(
                                new PredicateBuilder(rowType)
                                        .equal(1, BinaryString.fromString("b")))
                        .createReader(plan.splits());
        reader.forEachRemaining(row -> assertThat(row.getString(1).toString()).isEqualTo("b"));
    }

    @Test
    public void testBSIAndBitmapIndexInMemory() throws Exception {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("event", DataTypes.STRING())
                        .field("price", DataTypes.BIGINT())
                        .build();
        // in unaware-bucket mode, we split files into splits all the time
        FileStoreTable table =
                createUnawareBucketFileStoreTable(
                        rowType,
                        options -> {
                            options.set(METADATA_STATS_MODE, "NONE");
                            options.set(
                                    FileIndexOptions.FILE_INDEX
                                            + "."
                                            + BitmapFileIndexFactory.BITMAP_INDEX
                                            + "."
                                            + CoreOptions.COLUMNS,
                                    "event");
                            options.set(
                                    FileIndexOptions.FILE_INDEX
                                            + "."
                                            + BitSliceIndexBitmapFileIndexFactory.BSI_INDEX
                                            + "."
                                            + CoreOptions.COLUMNS,
                                    "price");
                            options.set(FILE_INDEX_IN_MANIFEST_THRESHOLD.key(), "1 MB");
                        });

        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        List<CommitMessage> result = new ArrayList<>();
        write.write(GenericRow.of(1, BinaryString.fromString("A"), 4L));
        write.write(GenericRow.of(1, BinaryString.fromString("B"), 2L));
        write.write(GenericRow.of(1, BinaryString.fromString("B"), 3L));
        write.write(GenericRow.of(1, BinaryString.fromString("C"), 3L));
        result.addAll(write.prepareCommit(true, 0));
        write.write(GenericRow.of(1, BinaryString.fromString("A"), 4L));
        write.write(GenericRow.of(1, BinaryString.fromString("B"), 3L));
        write.write(GenericRow.of(1, BinaryString.fromString("C"), 4L));
        write.write(GenericRow.of(1, BinaryString.fromString("D"), 2L));
        write.write(GenericRow.of(1, BinaryString.fromString("D"), 4L));
        result.addAll(write.prepareCommit(true, 0));
        commit.commit(0, result);
        result.clear();

        // test bitmap index and bsi index
        Predicate predicate =
                PredicateBuilder.and(
                        new PredicateBuilder(rowType).equal(1, BinaryString.fromString("C")),
                        new PredicateBuilder(rowType).greaterThan(2, 3L));
        TableScan.Plan plan = table.newScan().withFilter(predicate).plan();
        List<DataFileMeta> metas =
                plan.splits().stream()
                        .flatMap(split -> ((DataSplit) split).dataFiles().stream())
                        .collect(Collectors.toList());
        assertThat(metas.size()).isEqualTo(1);

        RecordReader<InternalRow> reader =
                table.newRead().withFilter(predicate).createReader(plan.splits());
        reader.forEachRemaining(
                row -> {
                    assertThat(row.getString(1).toString()).isEqualTo("C");
                    assertThat(row.getLong(2)).isEqualTo(4L);
                });
    }

    @Test
    public void testBSIAndBitmapIndexInDisk() throws Exception {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("event", DataTypes.STRING())
                        .field("price", DataTypes.BIGINT())
                        .build();
        // in unaware-bucket mode, we split files into splits all the time
        FileStoreTable table =
                createUnawareBucketFileStoreTable(
                        rowType,
                        options -> {
                            options.set(METADATA_STATS_MODE, "NONE");
                            options.set(
                                    FileIndexOptions.FILE_INDEX
                                            + "."
                                            + BitmapFileIndexFactory.BITMAP_INDEX
                                            + "."
                                            + CoreOptions.COLUMNS,
                                    "event");
                            options.set(
                                    FileIndexOptions.FILE_INDEX
                                            + "."
                                            + BitSliceIndexBitmapFileIndexFactory.BSI_INDEX
                                            + "."
                                            + CoreOptions.COLUMNS,
                                    "price");
                            options.set(FILE_INDEX_IN_MANIFEST_THRESHOLD.key(), "1 B");
                        });

        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        List<CommitMessage> result = new ArrayList<>();
        write.write(GenericRow.of(1, BinaryString.fromString("A"), 4L));
        write.write(GenericRow.of(1, BinaryString.fromString("B"), 2L));
        write.write(GenericRow.of(1, BinaryString.fromString("B"), 3L));
        write.write(GenericRow.of(1, BinaryString.fromString("C"), 3L));
        result.addAll(write.prepareCommit(true, 0));
        write.write(GenericRow.of(1, BinaryString.fromString("A"), 4L));
        write.write(GenericRow.of(1, BinaryString.fromString("B"), 3L));
        write.write(GenericRow.of(1, BinaryString.fromString("C"), 4L));
        write.write(GenericRow.of(1, BinaryString.fromString("D"), 2L));
        write.write(GenericRow.of(1, BinaryString.fromString("D"), 4L));
        result.addAll(write.prepareCommit(true, 0));
        commit.commit(0, result);
        result.clear();

        // test bitmap index and bsi index
        Predicate predicate =
                PredicateBuilder.and(
                        new PredicateBuilder(rowType).equal(1, BinaryString.fromString("C")),
                        new PredicateBuilder(rowType).greaterThan(2, 3L));
        TableScan.Plan plan = table.newScan().withFilter(predicate).plan();
        List<DataFileMeta> metas =
                plan.splits().stream()
                        .flatMap(split -> ((DataSplit) split).dataFiles().stream())
                        .collect(Collectors.toList());
        assertThat(metas.size()).isEqualTo(2);

        RecordReader<InternalRow> reader =
                table.newRead().withFilter(predicate).createReader(plan.splits());
        reader.forEachRemaining(
                row -> {
                    assertThat(row.getString(1).toString()).isEqualTo("C");
                    assertThat(row.getLong(2)).isEqualTo(4L);
                });
    }

    @Test
    public void testBitmapIndexResultFilterParquetRowRanges() throws Exception {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.STRING())
                        .field("event", DataTypes.STRING())
                        .field("price", DataTypes.INT())
                        .build();
        // in unaware-bucket mode, we split files into splits all the time
        FileStoreTable table =
                createUnawareBucketFileStoreTable(
                        rowType,
                        options -> {
                            options.set(FILE_FORMAT, FILE_FORMAT_PARQUET);
                            options.set(WRITE_ONLY, true);
                            options.set(
                                    FileIndexOptions.FILE_INDEX
                                            + "."
                                            + BitSliceIndexBitmapFileIndexFactory.BSI_INDEX
                                            + "."
                                            + CoreOptions.COLUMNS,
                                    "price");
                            options.set("parquet.block.size", "1048576");
                            options.set("parquet.page.size.row.check.min", "100");
                            options.set("parquet.page.row.count.limit", "300");
                        });

        int bound = 300000;
        Random random = new Random();
        Map<Integer, Integer> expectedMap = new HashMap<>();
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);
        for (int j = 0; j < 1000000; j++) {
            int next = random.nextInt(bound);
            BinaryString uuid = BinaryString.fromString(UUID.randomUUID().toString());
            expectedMap.compute(next, (key, value) -> value == null ? 1 : value + 1);
            write.write(GenericRow.of(uuid, uuid, next));
        }
        commit.commit(0, write.prepareCommit(true, 0));
        write.close();
        commit.close();

        // test eq
        for (int i = 0; i < 10; i++) {
            int key = random.nextInt(bound);
            Predicate predicate = new PredicateBuilder(rowType).equal(2, key);
            TableScan.Plan plan = table.newScan().plan();
            RecordReader<InternalRow> reader =
                    table.newRead().withFilter(predicate).createReader(plan.splits());
            AtomicInteger cnt = new AtomicInteger(0);
            reader.forEachRemaining(
                    row -> {
                        cnt.incrementAndGet();
                        assertThat(row.getInt(2)).isEqualTo(key);
                    });
            assertThat(cnt.get()).isEqualTo(expectedMap.getOrDefault(key, 0));
            reader.close();
        }

        //  test between
        for (int i = 0; i < 10; i++) {
            int max = random.nextInt(bound) + 1;
            int min = random.nextInt(max);
            Predicate predicate =
                    PredicateBuilder.and(
                            new PredicateBuilder(rowType).greaterOrEqual(2, min),
                            new PredicateBuilder(rowType).lessOrEqual(2, max));
            TableScan.Plan plan = table.newScan().plan();
            RecordReader<InternalRow> reader =
                    table.newRead().withFilter(predicate).createReader(plan.splits());
            AtomicInteger cnt = new AtomicInteger(0);
            reader.forEachRemaining(
                    row -> {
                        cnt.addAndGet(1);
                        assertThat(row.getInt(2)).isGreaterThanOrEqualTo(min);
                        assertThat(row.getInt(2)).isLessThanOrEqualTo(max);
                    });
            Optional<Integer> reduce =
                    expectedMap.entrySet().stream()
                            .filter(x -> x.getKey() >= min && x.getKey() <= max)
                            .map(Map.Entry::getValue)
                            .reduce(Integer::sum);
            assertThat(cnt.get()).isEqualTo(reduce.orElse(0));
            reader.close();
        }
    }

    @Test
    public void testTopNResultFilterParquetRowRanges() throws Exception {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.STRING())
                        .field("event", DataTypes.STRING())
                        .field("price", DataTypes.INT())
                        .build();
        Consumer<Options> configure =
                options -> {
                    options.set(FILE_FORMAT, FILE_FORMAT_PARQUET);
                    options.set(WRITE_ONLY, true);
                    options.set(
                            FileIndexOptions.FILE_INDEX
                                    + "."
                                    + RangeBitmapFileIndexFactory.RANGE_BITMAP
                                    + "."
                                    + CoreOptions.COLUMNS,
                            "price");
                    options.set("parquet.block.size", "1048576");
                    options.set("parquet.page.size.row.check.min", "100");
                    options.set("parquet.page.row.count.limit", "300");
                };
        // in unaware-bucket mode, we split files into splits all the time
        FileStoreTable table = createUnawareBucketFileStoreTable(rowType, configure);

        int bound = 30000000;
        int rowCount = 1000000;
        Random random = new Random();
        int k = random.nextInt(100) + 1;
        PriorityQueue<Integer> expected = new PriorityQueue<>(k, Integer::compareTo);
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);
        for (int j = 0; j < rowCount; j++) {
            int next = random.nextInt(bound);
            BinaryString uuid = BinaryString.fromString(UUID.randomUUID().toString());
            write.write(GenericRow.of(uuid, uuid, next));

            // TopK expected
            if (expected.size() < k) {
                expected.offer(next);
            } else if (expected.peek() <= next) {
                expected.poll();
                expected.offer(next);
            }
        }
        commit.commit(0, write.prepareCommit(true, 0));
        write.close();
        commit.close();

        // test TopK index
        {
            RoaringBitmap32 bitmap = new RoaringBitmap32();
            expected.forEach(bitmap::add);
            DataField field = rowType.getField("price");
            FieldRef ref = new FieldRef(field.id(), field.name(), field.type());
            TopN topN = new TopN(ref, DESCENDING, NULLS_LAST, k);
            TableScan.Plan plan = table.newScan().withTopN(topN).plan();
            RecordReader<InternalRow> reader =
                    table.newRead().withTopN(topN).createReader(plan.splits());
            AtomicInteger cnt = new AtomicInteger(0);
            RoaringBitmap32 actual = new RoaringBitmap32();
            reader.forEachRemaining(
                    row -> {
                        cnt.incrementAndGet();
                        actual.add(row.getInt(2));
                    });
            assertThat(cnt.get()).isEqualTo(k);
            assertThat(actual).isEqualTo(bitmap);
            reader.close();
        }

        // test TopK without index
        {
            DataField field = rowType.getField("id");
            FieldRef ref = new FieldRef(field.id(), field.name(), field.type());
            TopN topN = new TopN(ref, DESCENDING, NULLS_LAST, k);
            TableScan.Plan plan = table.newScan().withTopN(topN).plan();
            RecordReader<InternalRow> reader =
                    table.newRead().withTopN(topN).createReader(plan.splits());
            AtomicInteger cnt = new AtomicInteger(0);
            reader.forEachRemaining(row -> cnt.incrementAndGet());
            assertThat(cnt.get()).isEqualTo(rowCount);
            reader.close();
        }

        // test should not push topN with index and evolution
        {
            table.schemaManager()
                    .commitChanges(SchemaChange.updateColumnType("price", DataTypes.BIGINT()));
            rowType =
                    RowType.builder()
                            .field("id", DataTypes.STRING())
                            .field("event", DataTypes.STRING())
                            .field("price", DataTypes.BIGINT())
                            .build();
            table = createUnawareBucketFileStoreTable(rowType, configure);
            DataField field = rowType.getField("price");
            FieldRef ref = new FieldRef(field.id(), field.name(), field.type());
            TopN topN = new TopN(ref, DESCENDING, NULLS_LAST, k);
            TableScan.Plan plan = table.newScan().plan();
            RecordReader<InternalRow> reader =
                    table.newRead().withTopN(topN).createReader(plan.splits());
            AtomicInteger cnt = new AtomicInteger(0);
            reader.forEachRemaining(row -> cnt.incrementAndGet());
            assertThat(cnt.get()).isEqualTo(rowCount);
            reader.close();
        }
    }

    @Test
    public void testLimitPushDown() throws Exception {
        RowType rowType = RowType.builder().field("id", DataTypes.INT()).build();
        Consumer<Options> configure =
                options -> {
                    options.set(FILE_FORMAT, FILE_FORMAT_PARQUET);
                    options.set(WRITE_ONLY, true);
                    options.set(SOURCE_SPLIT_TARGET_SIZE, MemorySize.ofBytes(1));
                    options.set("parquet.block.size", "1048576");
                    options.set("parquet.page.size.row.check.min", "100");
                    options.set("parquet.page.row.count.limit", "300");
                };
        // in unaware-bucket mode, we split files into splits all the time
        FileStoreTable table = createUnawareBucketFileStoreTable(rowType, configure);

        int rowCount = 10000;
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);
        for (int i = 0; i < rowCount; i++) {
            write.write(GenericRow.of(i));
        }
        commit.commit(0, write.prepareCommit(true, 0));

        for (int i = 0; i < rowCount; i++) {
            write.write(GenericRow.of(i));
        }
        commit.commit(1, write.prepareCommit(true, 1));

        for (int i = 0; i < rowCount; i++) {
            write.write(GenericRow.of(i));
        }
        commit.commit(2, write.prepareCommit(true, 2));

        write.close();
        commit.close();

        // test limit push down
        {
            int limit = new RandomDataGenerator().nextInt(1, 1000);
            TableScan.Plan plan = table.newScan().withLimit(limit).plan();
            assertThat(plan.splits()).hasSize(1);

            RecordReader<InternalRow> reader =
                    table.newRead().withLimit(limit).createReader(plan.splits());
            AtomicInteger cnt = new AtomicInteger(0);
            reader.forEachRemaining(row -> cnt.incrementAndGet());
            assertThat(cnt.get()).isEqualTo(limit);
            reader.close();
        }

        // avoid unstable failure from `SimpleTableTestBase.after`.
        Thread.sleep(1_000);
    }

    @Test
    public void testLimitWithCloseableIterator() throws Exception {
        RowType rowType = RowType.builder().field("id", DataTypes.INT()).build();
        Consumer<Options> configure =
                options -> {
                    options.set(FILE_FORMAT, FILE_FORMAT_PARQUET);
                    options.set(WRITE_ONLY, true);
                    options.set(SOURCE_SPLIT_TARGET_SIZE, MemorySize.ofMebiBytes(256));
                };
        FileStoreTable table = createUnawareBucketFileStoreTable(rowType, configure);

        int rowCount = 5000;
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);
        for (int i = 0; i < rowCount; i++) {
            write.write(GenericRow.of(i));
        }
        commit.commit(0, write.prepareCommit(true, 0));
        write.close();
        commit.close();

        int limit = 10;
        TableScan.Plan plan = table.newScan().withLimit(limit).plan();
        RecordReader<InternalRow> reader =
                table.newRead().withLimit(limit).createReader(plan.splits());
        AtomicInteger count = new AtomicInteger(0);
        try (CloseableIterator<InternalRow> iterator = reader.toCloseableIterator()) {
            while (iterator.hasNext()) {
                iterator.next();
                count.incrementAndGet();
            }
        }
        assertThat(count.get()).isEqualTo(limit);

        Thread.sleep(1_000);
    }

    @Test
    public void testWithShardAppendTable() throws Exception {
        FileStoreTable table = createFileStoreTable(conf -> conf.set(BUCKET, -1));
        innerTestWithShard(table);
    }

    @Test
    public void testWithShardBucketedTable() throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        conf -> {
                            conf.set(BUCKET, 5);
                            conf.set(BUCKET_KEY, "a");
                        });
        innerTestWithShard(table);
    }

    @Test
    public void testBloomFilterForMapField() throws Exception {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("index_column", DataTypes.STRING())
                        .field("index_column2", DataTypes.INT())
                        .field(
                                "index_column3",
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                        .build();
        // in unaware-bucket mode, we split files into splits all the time
        FileStoreTable table =
                createUnawareBucketFileStoreTable(
                        rowType,
                        options -> {
                            options.set(
                                    FileIndexOptions.FILE_INDEX
                                            + "."
                                            + BloomFilterFileIndexFactory.BLOOM_FILTER
                                            + "."
                                            + CoreOptions.COLUMNS,
                                    "index_column, index_column2, index_column3[a], index_column3[b], index_column3[c], index_column3[d]");
                            options.set(
                                    FileIndexOptions.FILE_INDEX
                                            + "."
                                            + BloomFilterFileIndexFactory.BLOOM_FILTER
                                            + ".index_column.items",
                                    "150");
                            options.set(
                                    FileIndexOptions.FILE_INDEX
                                            + "."
                                            + BloomFilterFileIndexFactory.BLOOM_FILTER
                                            + ".index_column2.items",
                                    "150");
                            options.set(
                                    FileIndexOptions.FILE_INDEX
                                            + "."
                                            + BloomFilterFileIndexFactory.BLOOM_FILTER
                                            + ".index_column3.items",
                                    "150");
                            options.set(
                                    FileIndexOptions.FILE_INDEX
                                            + "."
                                            + BloomFilterFileIndexFactory.BLOOM_FILTER
                                            + ".index_column3[a].items",
                                    "10000");
                        });

        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);
        List<CommitMessage> result = new ArrayList<>();
        write.write(
                GenericRow.of(
                        1,
                        BinaryString.fromString("a"),
                        2,
                        new GenericMap(
                                new HashMap<BinaryString, BinaryString>() {
                                    {
                                        put(
                                                BinaryString.fromString("a"),
                                                BinaryString.fromString("10086"));
                                        put(
                                                BinaryString.fromString("b"),
                                                BinaryString.fromString("1008611"));
                                        put(
                                                BinaryString.fromString("c"),
                                                BinaryString.fromString("1008612"));
                                        put(
                                                BinaryString.fromString("d"),
                                                BinaryString.fromString("1008613"));
                                    }
                                })));
        write.write(
                GenericRow.of(
                        1,
                        BinaryString.fromString("c"),
                        2,
                        new GenericMap(
                                new HashMap<BinaryString, BinaryString>() {
                                    {
                                        put(
                                                BinaryString.fromString("a"),
                                                BinaryString.fromString("我是一个粉刷匠"));
                                        put(
                                                BinaryString.fromString("b"),
                                                BinaryString.fromString("啦啦啦"));
                                        put(
                                                BinaryString.fromString("c"),
                                                BinaryString.fromString("快乐的粉刷匠"));
                                        put(
                                                BinaryString.fromString("d"),
                                                BinaryString.fromString("大风大雨去刷墙"));
                                    }
                                })));
        result.addAll(write.prepareCommit(true, 0));
        write.write(
                GenericRow.of(
                        1,
                        BinaryString.fromString("b"),
                        2,
                        new GenericMap(
                                new HashMap<BinaryString, BinaryString>() {
                                    {
                                        put(
                                                BinaryString.fromString("a"),
                                                BinaryString.fromString("I am a good girl"));
                                        put(
                                                BinaryString.fromString("b"),
                                                BinaryString.fromString("A good girl"));
                                        put(
                                                BinaryString.fromString("c"),
                                                BinaryString.fromString("Good girl"));
                                        put(
                                                BinaryString.fromString("d"),
                                                BinaryString.fromString("Girl"));
                                    }
                                })));
        result.addAll(write.prepareCommit(true, 0));
        commit.commit(0, result);
        result.clear();
        Predicate predicate =
                new LeafPredicate(
                        Equal.INSTANCE,
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()),
                        3,
                        "index_column3[a]",
                        Collections.singletonList(BinaryString.fromString("I am a good girl")));
        TableScan.Plan plan = table.newScan().withFilter(predicate).plan();
        List<DataFileMeta> metas =
                plan.splits().stream()
                        .flatMap(split -> ((DataSplit) split).dataFiles().stream())
                        .collect(Collectors.toList());
        assertThat(metas.size()).isEqualTo(2);

        RecordReader<InternalRow> reader =
                table.newRead().withFilter(predicate).createReader(plan.splits());
        reader.forEachRemaining(row -> assertThat(row.getString(1).toString()).isEqualTo("b"));
    }

    @Test
    public void testStreamingProjection() throws Exception {
        writeData();
        FileStoreTable table = createFileStoreTable();

        List<Split> splits =
                toSplits(table.newSnapshotReader().withMode(ScanMode.DELTA).read().dataSplits());
        TableRead read = table.newRead().withProjection(PROJECTION);
        assertThat(getResult(read, splits, binaryRow(1), 0, STREAMING_PROJECTED_ROW_TO_STRING))
                .isEqualTo(Arrays.asList("+101|11", "+102|12"));
        assertThat(getResult(read, splits, binaryRow(2), 0, STREAMING_PROJECTED_ROW_TO_STRING))
                .isEqualTo(Collections.singletonList("+201|21"));
    }

    @Test
    public void testStreamingFilter() throws Exception {
        writeData();
        FileStoreTable table = createFileStoreTable();
        PredicateBuilder builder = new PredicateBuilder(table.schema().logicalRowType());

        Predicate predicate = builder.equal(2, 101L);
        List<Split> splits =
                toSplits(
                        table.newSnapshotReader()
                                .withMode(ScanMode.DELTA)
                                .withFilter(predicate)
                                .read()
                                .dataSplits());
        TableRead read = table.newRead();
        assertThat(getResult(read, splits, binaryRow(1), 0, STREAMING_ROW_TO_STRING))
                .isEqualTo(
                        Arrays.asList(
                                "+1|11|101|binary|varbinary|mapKey:mapVal|multiset",
                                // this record is in the same file with "+1|11|101"
                                "+1|12|102|binary|varbinary|mapKey:mapVal|multiset"));
        assertThat(getResult(read, splits, binaryRow(2), 0, STREAMING_ROW_TO_STRING)).isEmpty();
    }

    @Test
    public void testSequentialRead() throws Exception {
        Random random = new Random();
        int numOfBucket = Math.max(random.nextInt(8), 1);
        FileStoreTable table = createFileStoreTable(numOfBucket);
        InternalRowSerializer serializer =
                new InternalRowSerializer(table.schema().logicalRowType());
        StreamTableWrite write = table.newWrite(commitUser);

        StreamTableCommit commit = table.newCommit(commitUser);
        List<Map<Integer, List<InternalRow>>> dataset = new ArrayList<>();
        Map<Integer, List<InternalRow>> dataPerBucket = new HashMap<>(numOfBucket);
        int numOfPartition = Math.max(random.nextInt(10), 1);
        DefaultBucketFunction bucketFunction = new DefaultBucketFunction();
        for (int i = 0; i < numOfPartition; i++) {
            for (int j = 0; j < Math.max(random.nextInt(200), 1); j++) {
                BinaryRow data =
                        serializer
                                .toBinaryRow(rowData(i, random.nextInt(), random.nextLong()))
                                .copy();
                int bucket = bucketFunction.bucket(row(data.getInt(1)), numOfBucket);
                dataPerBucket.compute(
                        bucket,
                        (k, v) -> {
                            if (v == null) {
                                v = new ArrayList<>();
                            }
                            v.add(data);
                            return v;
                        });
                write.write(data);
            }
            dataset.add(new HashMap<>(dataPerBucket));
            dataPerBucket.clear();
        }
        commit.commit(0, write.prepareCommit(true, 0));
        write.close();
        commit.close();

        int partition = random.nextInt(numOfPartition);
        List<Integer> availableBucket = new ArrayList<>(dataset.get(partition).keySet());
        int bucket = availableBucket.get(random.nextInt(availableBucket.size()));

        Predicate partitionFilter =
                new PredicateBuilder(table.schema().logicalRowType()).equal(0, partition);
        List<Split> splits =
                toSplits(
                        table.newSnapshotReader()
                                .withFilter(partitionFilter)
                                .withBucket(bucket)
                                .read()
                                .dataSplits());
        TableRead read = table.newRead();

        assertThat(getResult(read, splits, binaryRow(partition), bucket, STREAMING_ROW_TO_STRING))
                .containsExactlyElementsOf(
                        dataset.get(partition).get(bucket).stream()
                                .map(STREAMING_ROW_TO_STRING)
                                .collect(Collectors.toList()));
    }

    @Test
    public void testBatchOrderWithCompaction() throws Exception {
        FileStoreTable table = createFileStoreTable();
        table = table.copy(Collections.singletonMap(CoreOptions.ASYNC_FILE_WRITE.key(), "false"));

        int number = 61;
        List<Integer> expected = new ArrayList<>();

        {
            StreamTableWrite write = table.newWrite(commitUser);
            StreamTableCommit commit = table.newCommit(commitUser);

            for (int i = 0; i < number; i++) {
                write.write(rowData(1, i, (long) i));
                commit.commit(i, write.prepareCommit(false, i));
                expected.add(i);
            }
            write.close();
            commit.close();

            ReadBuilder readBuilder = table.newReadBuilder();
            List<Split> splits = readBuilder.newScan().plan().splits();
            List<Integer> result = new ArrayList<>();
            readBuilder
                    .newRead()
                    .createReader(splits)
                    .forEachRemaining(r -> result.add(r.getInt(1)));
            assertThat(result).containsExactlyElementsOf(expected);
        }

        // restore
        {
            StreamTableWrite write = table.newWrite(commitUser);
            StreamTableCommit commit = table.newCommit(commitUser);

            for (int i = number; i < number + 51; i++) {
                write.write(rowData(1, i, (long) i));
                commit.commit(i, write.prepareCommit(false, i));
                expected.add(i);
            }
            write.close();
            commit.close();

            ReadBuilder readBuilder = table.newReadBuilder();
            List<Split> splits = readBuilder.newScan().plan().splits();
            List<Integer> result = new ArrayList<>();
            readBuilder
                    .newRead()
                    .createReader(splits)
                    .forEachRemaining(r -> result.add(r.getInt(1)));
            assertThat(result).containsExactlyElementsOf(expected);
        }
    }

    @Test
    public void testEqualsAndHashCode() throws Exception {
        // Test same table equals and hashCode consistency
        FileStoreTable table1 = createFileStoreTable();
        FileStoreTable table2 = table1.copy(table1.schema());
        assertThat(table1.equals(table2)).isTrue();
        assertThat(table1.hashCode()).isEqualTo(table2.hashCode());

        // Test with different options
        Map<String, String> optionsWithMock = new HashMap<>(table1.schema().options());
        optionsWithMock.put("mockKey", "mockValue");
        TableSchema schemaWithMock = table1.schema().copy(optionsWithMock);
        FileStoreTable tableWithMock = table1.copy(schemaWithMock);

        assertThat(table1.equals(tableWithMock)).isFalse();
        assertThat(table1.hashCode()).isNotEqualTo(tableWithMock.hashCode());

        // Test same options should be equal
        Map<String, String> sameOptionsWithMock = new HashMap<>(table1.schema().options());
        sameOptionsWithMock.put("mockKey", "mockValue");
        TableSchema sameSchemaWithMock = table1.schema().copy(sameOptionsWithMock);
        FileStoreTable sameTableWithMock = table1.copy(sameSchemaWithMock);

        assertThat(tableWithMock.equals(sameTableWithMock)).isTrue();
        assertThat(tableWithMock.hashCode()).isEqualTo(sameTableWithMock.hashCode());
    }

    private void writeData() throws Exception {
        writeData(options -> {});
    }

    private void writeData(Consumer<Options> optionsSetter) throws Exception {
        FileStoreTable table = createFileStoreTable(optionsSetter);
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        write.write(rowData(1, 10, 100L));
        write.write(rowData(2, 20, 200L));
        write.write(rowData(1, 11, 101L));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData(1, 12, 102L));
        write.write(rowData(2, 21, 201L));
        write.write(rowData(2, 22, 202L));
        commit.commit(1, write.prepareCommit(true, 1));

        write.write(rowData(1, 11, 101L));
        write.write(rowData(2, 21, 201L));
        write.write(rowData(1, 12, 102L));
        commit.commit(2, write.prepareCommit(true, 2));

        write.close();
        commit.close();
    }

    private void writeBranchData(FileStoreTable table) throws Exception {
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        write.write(rowData(1, 10, 100L));
        write.write(rowData(2, 20, 200L));
        write.write(rowData(1, 11, 101L));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData(1, 12, 102L));
        write.write(rowData(2, 21, 201L));
        write.write(rowData(2, 22, 202L));
        commit.commit(1, write.prepareCommit(true, 1));

        write.write(rowData(1, 11, 101L));
        write.write(rowData(2, 21, 201L));
        write.write(rowData(1, 12, 102L));
        commit.commit(2, write.prepareCommit(true, 2));

        write.close();
        commit.close();
    }

    @Override
    protected FileStoreTable createFileStoreTable(Consumer<Options> configure, RowType rowType)
            throws Exception {
        Options conf = new Options();
        conf.set(CoreOptions.PATH, tablePath.toString());
        configure.accept(conf);
        if (!conf.contains(BUCKET_KEY) && conf.get(BUCKET) != -1) {
            conf.set(BUCKET_KEY, "a");
        }
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), tablePath),
                        new Schema(
                                rowType.getFields(),
                                Collections.singletonList("pt"),
                                Collections.emptyList(),
                                conf.toMap(),
                                ""));
        return new AppendOnlyFileStoreTable(FileIOFinder.find(tablePath), tablePath, tableSchema);
    }

    protected FileStoreTable createUnawareBucketFileStoreTable(Consumer<Options> configure)
            throws Exception {
        Options conf = new Options();
        conf.set(CoreOptions.PATH, tablePath.toString());
        conf.set(CoreOptions.BUCKET, -1);
        configure.accept(conf);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), tablePath),
                        new Schema(
                                ROW_TYPE.getFields(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                conf.toMap(),
                                ""));
        return new AppendOnlyFileStoreTable(FileIOFinder.find(tablePath), tablePath, tableSchema);
    }

    protected FileStoreTable createUnawareBucketFileStoreTable(
            RowType rowType, Consumer<Options> configure) throws Exception {
        Options conf = new Options();
        conf.set(CoreOptions.PATH, tablePath.toString());
        conf.set(CoreOptions.BUCKET, -1);
        configure.accept(conf);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), tablePath),
                        new Schema(
                                rowType.getFields(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                conf.toMap(),
                                ""));
        return new AppendOnlyFileStoreTable(FileIOFinder.find(tablePath), tablePath, tableSchema);
    }

    protected FileStoreTable createBranchMergeTable() throws Exception {
        return createFileStoreTable();
    }

    protected FileStoreTable createBranchMergeTable(Consumer<Options> extraConfigure)
            throws Exception {
        return createFileStoreTable(extraConfigure);
    }

    @Test
    public void testMergeBranch() throws Exception {
        FileStoreTable table = createBranchMergeTable();

        // Write data to main
        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(0, 0, 0L));
            commit.commit(0, write.prepareCommit(false, 1));
        }

        // Create branch from tag
        table.createTag("tag1", 1);
        table.createBranch(BRANCH_NAME, "tag1");
        FileStoreTable tableBranch = table.switchToBranch(BRANCH_NAME);

        // Write data to branch
        try (StreamTableWrite write = tableBranch.newWrite(commitUser);
                StreamTableCommit commit = tableBranch.newCommit(commitUser)) {
            write.write(rowData(1, 10, 100L));
            commit.commit(1, write.prepareCommit(false, 2));
        }

        // Write more data to main
        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(2, 20, 200L));
            commit.commit(2, write.prepareCommit(false, 2));
        }

        // Merge branch into main
        table.mergeBranch(BRANCH_NAME, "main");

        // Verify main has data from both sides
        assertThat(
                        getResult(
                                table.newRead(),
                                toSplits(table.newSnapshotReader().read().dataSplits()),
                                BATCH_ROW_TO_STRING))
                .containsExactlyInAnyOrder(
                        "0|0|0|binary|varbinary|mapKey:mapVal|multiset",
                        "1|10|100|binary|varbinary|mapKey:mapVal|multiset",
                        "2|20|200|binary|varbinary|mapKey:mapVal|multiset");
    }

    @Test
    public void testMergeBranchMultipleTimes() throws Exception {
        FileStoreTable table = createBranchMergeTable();

        // Write data to main
        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(0, 0, 0L));
            commit.commit(0, write.prepareCommit(false, 1));
        }

        // Create branch from tag
        table.createTag("tag1", 1);
        table.createBranch(BRANCH_NAME, "tag1");
        FileStoreTable tableBranch = table.switchToBranch(BRANCH_NAME);

        // First write to branch
        try (StreamTableWrite write = tableBranch.newWrite(commitUser);
                StreamTableCommit commit = tableBranch.newCommit(commitUser)) {
            write.write(rowData(1, 10, 100L));
            commit.commit(1, write.prepareCommit(false, 2));
        }

        // First merge
        table.mergeBranch(BRANCH_NAME, "main");

        // Second write to branch
        try (StreamTableWrite write = tableBranch.newWrite(commitUser);
                StreamTableCommit commit = tableBranch.newCommit(commitUser)) {
            write.write(rowData(2, 20, 200L));
            commit.commit(2, write.prepareCommit(false, 3));
        }

        // Second merge
        table.mergeBranch(BRANCH_NAME, "main");

        // Verify no duplicates: main has all 3 rows exactly once
        assertThat(
                        getResult(
                                table.newRead(),
                                toSplits(table.newSnapshotReader().read().dataSplits()),
                                BATCH_ROW_TO_STRING))
                .containsExactlyInAnyOrder(
                        "0|0|0|binary|varbinary|mapKey:mapVal|multiset",
                        "1|10|100|binary|varbinary|mapKey:mapVal|multiset",
                        "2|20|200|binary|varbinary|mapKey:mapVal|multiset");
    }

    @Test
    public void testMergeBranchFailsOnStaleDuplicateCommit() throws Exception {
        FileStoreTable table = createBranchMergeTable();

        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(0, 0, 0L));
            commit.commit(0, write.prepareCommit(false, 1));
        }

        table.createBranch(BRANCH_NAME);
        FileStoreTable tableBranch = table.switchToBranch(BRANCH_NAME);
        try (StreamTableWrite write = tableBranch.newWrite(commitUser);
                StreamTableCommit commit = tableBranch.newCommit(commitUser)) {
            write.write(rowData(1, 10, 100L));
            commit.commit(1, write.prepareCommit(false, 2));
        }

        BranchMergeHandler handler = new BranchMergeHandler(table::switchToBranch);
        Map<org.apache.paimon.manifest.FileEntry.Identifier, ManifestEntry> sourceFiles =
                handler.readBranchFiles(BRANCH_NAME);
        Map<org.apache.paimon.manifest.FileEntry.Identifier, ManifestEntry> targetFiles =
                handler.readBranchFiles("main");
        List<ManifestEntry> filesToMerge =
                sourceFiles.entrySet().stream()
                        .filter(entry -> !targetFiles.containsKey(entry.getKey()))
                        .map(Map.Entry::getValue)
                        .collect(Collectors.toList());

        handler.commit("main", filesToMerge);
        assertThatThrownBy(() -> handler.commit("main", filesToMerge))
                .satisfies(anyCauseMatches(RuntimeException.class, "Trying to add file"));
    }

    @Test
    public void testMergeBranchBidirectional() throws Exception {
        FileStoreTable table = createBranchMergeTable();

        // Write shared data to main
        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(0, 0, 0L));
            commit.commit(0, write.prepareCommit(false, 1));
        }

        // Create branch from tag
        table.createTag("tag1", 1);
        table.createBranch(BRANCH_NAME, "tag1");
        FileStoreTable tableBranch = table.switchToBranch(BRANCH_NAME);

        // Write to branch
        try (StreamTableWrite write = tableBranch.newWrite(commitUser);
                StreamTableCommit commit = tableBranch.newCommit(commitUser)) {
            write.write(rowData(1, 10, 100L));
            commit.commit(1, write.prepareCommit(false, 2));
        }

        // Write to main
        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(2, 20, 200L));
            commit.commit(2, write.prepareCommit(false, 2));
        }

        // Merge branch -> main
        table.mergeBranch(BRANCH_NAME, "main");

        // Merge main -> branch
        table.mergeBranch("main", BRANCH_NAME);

        // Verify both have the same data without duplicates
        List<String> mainData =
                getResult(
                        table.newRead(),
                        toSplits(table.newSnapshotReader().read().dataSplits()),
                        BATCH_ROW_TO_STRING);
        List<String> branchData =
                getResult(
                        tableBranch.newRead(),
                        toSplits(tableBranch.newSnapshotReader().read().dataSplits()),
                        BATCH_ROW_TO_STRING);

        assertThat(mainData)
                .containsExactlyInAnyOrder(
                        "0|0|0|binary|varbinary|mapKey:mapVal|multiset",
                        "1|10|100|binary|varbinary|mapKey:mapVal|multiset",
                        "2|20|200|binary|varbinary|mapKey:mapVal|multiset");
        assertThat(branchData).containsExactlyInAnyOrderElementsOf(mainData);
    }

    @Test
    public void testMergeBranchEmptyDiff() throws Exception {
        FileStoreTable table = createBranchMergeTable();

        // Write data to main
        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(0, 0, 0L));
            commit.commit(0, write.prepareCommit(false, 1));
        }

        // Create branch from tag (has same data as main)
        table.createTag("tag1", 1);
        table.createBranch(BRANCH_NAME, "tag1");

        // Merge should be a no-op (no exception, no new snapshot)
        long snapshotBefore = table.snapshotManager().latestSnapshotId();
        table.mergeBranch(BRANCH_NAME, "main");
        long snapshotAfter = table.snapshotManager().latestSnapshotId();
        assertThat(snapshotAfter).isEqualTo(snapshotBefore);
    }

    @Test
    public void testMergeBranchSchemaConflict() throws Exception {
        FileStoreTable table = createBranchMergeTable();

        // Write data to main
        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(0, 0, 0L));
            commit.commit(0, write.prepareCommit(false, 1));
        }

        // Create branch from tag
        table.createTag("tag1", 1);
        table.createBranch(BRANCH_NAME, "tag1");

        // Modify schema on main (add a column)
        SchemaManager schemaManager = new SchemaManager(table.fileIO(), table.location());
        schemaManager.commitChanges(SchemaChange.addColumn("new_col", DataTypes.INT()));

        // Merge should fail due to schema mismatch
        assertThatThrownBy(() -> table.mergeBranch(BRANCH_NAME, "main"))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Cannot merge branch 'branch1' into 'main', schema mismatch."));
    }

    @Test
    public void testMergeBranchSchemaHistoryConflict() throws Exception {
        FileStoreTable table = createBranchMergeTable();

        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(0, 0, 0L));
            commit.commit(0, write.prepareCommit(false, 1));
        }

        table.createTag("tag1", 1);
        table.createBranch(BRANCH_NAME, "tag1");

        SchemaManager branchSchemaManager =
                new SchemaManager(table.fileIO(), table.location(), BRANCH_NAME);
        branchSchemaManager.commitChanges(SchemaChange.addColumn("source_col", DataTypes.INT()));
        FileStoreTable tableBranch = table.switchToBranch(BRANCH_NAME);
        try (BatchTableWrite write =
                        tableBranch.newBatchWriteBuilder().newWrite().withWriteType(ROW_TYPE);
                BatchTableCommit commit = tableBranch.newBatchWriteBuilder().newCommit()) {
            write.write(rowData(1, 10, 100L));
            commit.commit(write.prepareCommit());
        }
        branchSchemaManager.commitChanges(
                Collections.singletonList(SchemaChange.dropColumn("source_col")));

        SchemaManager mainSchemaManager = new SchemaManager(table.fileIO(), table.location());
        mainSchemaManager.commitChanges(SchemaChange.addColumn("target_col", DataTypes.INT()));
        mainSchemaManager.commitChanges(
                Collections.singletonList(SchemaChange.dropColumn("target_col")));

        assertThatThrownBy(() -> table.mergeBranch(BRANCH_NAME, "main"))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "schema history mismatch for schema id 1"));
    }

    @Test
    public void testMergeBranchRowTrackingTable() throws Exception {
        FileStoreTable table =
                createBranchMergeTable(
                        options -> options.set(CoreOptions.ROW_TRACKING_ENABLED, true));

        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(0, 0, 0L));
            commit.commit(0, write.prepareCommit(false, 1));
        }

        table.createTag("tag1", 1);
        table.createBranch(BRANCH_NAME, "tag1");
        FileStoreTable tableBranch = table.switchToBranch(BRANCH_NAME);

        try (StreamTableWrite write = tableBranch.newWrite(commitUser);
                StreamTableCommit commit = tableBranch.newCommit(commitUser)) {
            write.write(rowData(1, 10, 100L));
            commit.commit(1, write.prepareCommit(false, 2));
        }

        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(2, 20, 200L));
            commit.commit(2, write.prepareCommit(false, 2));
        }

        table.mergeBranch(BRANCH_NAME, "main");

        assertThat(
                        getResult(
                                table.newRead(),
                                toSplits(table.newSnapshotReader().read().dataSplits()),
                                BATCH_ROW_TO_STRING))
                .containsExactlyInAnyOrder(
                        "0|0|0|binary|varbinary|mapKey:mapVal|multiset",
                        "1|10|100|binary|varbinary|mapKey:mapVal|multiset",
                        "2|20|200|binary|varbinary|mapKey:mapVal|multiset");

        assertRowIdRangesNonOverlapping(table);
        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        assertThat(snapshot.nextRowId()).isEqualTo(3L);
    }

    @Test
    public void testMergeBranchRowTrackingMultipleTimes() throws Exception {
        FileStoreTable table =
                createBranchMergeTable(
                        options -> options.set(CoreOptions.ROW_TRACKING_ENABLED, true));

        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(0, 0, 0L));
            commit.commit(0, write.prepareCommit(false, 1));
        }

        table.createTag("tag1", 1);
        table.createBranch(BRANCH_NAME, "tag1");
        FileStoreTable tableBranch = table.switchToBranch(BRANCH_NAME);

        // First write to branch + merge
        try (StreamTableWrite write = tableBranch.newWrite(commitUser);
                StreamTableCommit commit = tableBranch.newCommit(commitUser)) {
            write.write(rowData(1, 10, 100L));
            commit.commit(1, write.prepareCommit(false, 2));
        }
        table.mergeBranch(BRANCH_NAME, "main");

        // Second write to branch + merge
        try (StreamTableWrite write = tableBranch.newWrite(commitUser);
                StreamTableCommit commit = tableBranch.newCommit(commitUser)) {
            write.write(rowData(2, 20, 200L));
            commit.commit(2, write.prepareCommit(false, 3));
        }
        table.mergeBranch(BRANCH_NAME, "main");

        assertThat(
                        getResult(
                                table.newRead(),
                                toSplits(table.newSnapshotReader().read().dataSplits()),
                                BATCH_ROW_TO_STRING))
                .containsExactlyInAnyOrder(
                        "0|0|0|binary|varbinary|mapKey:mapVal|multiset",
                        "1|10|100|binary|varbinary|mapKey:mapVal|multiset",
                        "2|20|200|binary|varbinary|mapKey:mapVal|multiset");

        assertRowIdRangesNonOverlapping(table);
        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        assertThat(snapshot.nextRowId()).isEqualTo(3L);
    }

    @Test
    public void testMergeBranchRowTrackingAfterTargetWrites() throws Exception {
        FileStoreTable table =
                createBranchMergeTable(
                        options -> options.set(CoreOptions.ROW_TRACKING_ENABLED, true));

        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(0, 0, 0L));
            commit.commit(0, write.prepareCommit(false, 1));
        }

        table.createTag("tag1", 1);
        table.createBranch(BRANCH_NAME, "tag1");
        FileStoreTable tableBranch = table.switchToBranch(BRANCH_NAME);

        // Write 2 rows to branch
        try (StreamTableWrite write = tableBranch.newWrite(commitUser);
                StreamTableCommit commit = tableBranch.newCommit(commitUser)) {
            write.write(rowData(1, 10, 100L));
            write.write(rowData(1, 11, 101L));
            commit.commit(1, write.prepareCommit(false, 2));
        }

        // Write 3 rows to main independently (advances main nextRowId)
        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(2, 20, 200L));
            write.write(rowData(2, 21, 201L));
            write.write(rowData(2, 22, 202L));
            commit.commit(2, write.prepareCommit(false, 2));
        }

        table.mergeBranch(BRANCH_NAME, "main");

        assertThat(
                        getResult(
                                table.newRead(),
                                toSplits(table.newSnapshotReader().read().dataSplits()),
                                BATCH_ROW_TO_STRING))
                .hasSize(6);

        assertRowIdRangesNonOverlapping(table);
        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        assertThat(snapshot.nextRowId()).isEqualTo(6L);
    }

    @Test
    public void testMergeBranchRowTrackingBetweenNonMainBranches() throws Exception {
        FileStoreTable table =
                createBranchMergeTable(
                        options -> options.set(CoreOptions.ROW_TRACKING_ENABLED, true));

        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(0, 0, 0L));
            commit.commit(0, write.prepareCommit(false, 1));
        }

        table.createTag("tag1", 1);
        table.createBranch("branchA", "tag1");
        table.createBranch("branchB", "tag1");
        FileStoreTable tableA = table.switchToBranch("branchA");
        FileStoreTable tableB = table.switchToBranch("branchB");

        try (StreamTableWrite write = tableA.newWrite(commitUser);
                StreamTableCommit commit = tableA.newCommit(commitUser)) {
            write.write(rowData(1, 10, 100L));
            commit.commit(1, write.prepareCommit(false, 2));
        }

        try (StreamTableWrite write = tableB.newWrite(commitUser);
                StreamTableCommit commit = tableB.newCommit(commitUser)) {
            write.write(rowData(2, 20, 200L));
            commit.commit(2, write.prepareCommit(false, 2));
        }

        table.mergeBranch("branchA", "branchB");

        tableB = table.switchToBranch("branchB");
        assertThat(
                        getResult(
                                tableB.newRead(),
                                toSplits(tableB.newSnapshotReader().read().dataSplits()),
                                BATCH_ROW_TO_STRING))
                .containsExactlyInAnyOrder(
                        "0|0|0|binary|varbinary|mapKey:mapVal|multiset",
                        "1|10|100|binary|varbinary|mapKey:mapVal|multiset",
                        "2|20|200|binary|varbinary|mapKey:mapVal|multiset");

        assertRowIdRangesNonOverlapping(tableB);
        Snapshot snapshot = tableB.snapshotManager().latestSnapshot();
        assertThat(snapshot.nextRowId()).isEqualTo(3L);
    }

    @Test
    public void testMergeBranchRowTrackingMismatch() throws Exception {
        FileStoreTable table =
                createBranchMergeTable(
                        options -> options.set(CoreOptions.ROW_TRACKING_ENABLED, true));

        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(0, 0, 0L));
            commit.commit(0, write.prepareCommit(false, 1));
        }

        table.createTag("tag1", 1);
        table.createBranch(BRANCH_NAME, "tag1");
        FileStoreTable tableBranch = table.switchToBranch(BRANCH_NAME);

        try (StreamTableWrite write = tableBranch.newWrite(commitUser);
                StreamTableCommit commit = tableBranch.newCommit(commitUser)) {
            write.write(rowData(1, 10, 100L));
            commit.commit(1, write.prepareCommit(false, 2));
        }

        // Directly write a new schema to the branch with row-tracking disabled
        SchemaManager branchSchemaManager =
                new SchemaManager(table.fileIO(), table.location(), BRANCH_NAME);
        TableSchema branchSchema = branchSchemaManager.latest().get();
        Map<String, String> newOptions = new HashMap<>(branchSchema.options());
        newOptions.remove("row-tracking.enabled");
        TableSchema mismatchedSchema =
                new TableSchema(
                        branchSchema.version(),
                        branchSchema.id() + 1,
                        branchSchema.fields(),
                        branchSchema.highestFieldId(),
                        branchSchema.partitionKeys(),
                        branchSchema.primaryKeys(),
                        newOptions,
                        branchSchema.comment(),
                        branchSchema.timeMillis());
        branchSchemaManager.commit(mismatchedSchema);

        assertThatThrownBy(() -> table.mergeBranch(BRANCH_NAME, "main"))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "row-tracking settings must match"));
    }

    @Test
    public void testMergeBranchRowTrackingStaleMerge() throws Exception {
        FileStoreTable table =
                createBranchMergeTable(
                        options -> options.set(CoreOptions.ROW_TRACKING_ENABLED, true));

        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(0, 0, 0L));
            commit.commit(0, write.prepareCommit(false, 1));
        }

        table.createTag("tag1", 1);
        table.createBranch(BRANCH_NAME, "tag1");
        FileStoreTable tableBranch = table.switchToBranch(BRANCH_NAME);

        // Write to branch
        try (StreamTableWrite write = tableBranch.newWrite(commitUser);
                StreamTableCommit commit = tableBranch.newCommit(commitUser)) {
            write.write(rowData(1, 10, 100L));
            commit.commit(1, write.prepareCommit(false, 2));
        }

        // Write to main multiple times to advance nextRowId
        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(2, 20, 200L));
            commit.commit(2, write.prepareCommit(false, 2));
        }
        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(3, 30, 300L));
            write.write(rowData(3, 31, 301L));
            commit.commit(3, write.prepareCommit(false, 3));
        }

        // Merge: branch file should get firstRowId after all main files
        table.mergeBranch(BRANCH_NAME, "main");

        assertThat(
                        getResult(
                                table.newRead(),
                                toSplits(table.newSnapshotReader().read().dataSplits()),
                                BATCH_ROW_TO_STRING))
                .hasSize(5);

        assertRowIdRangesNonOverlapping(table);
        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        assertThat(snapshot.nextRowId()).isEqualTo(5L);

        // Write more to main, then merge again (branch has no new data, should be no-op)
        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(4, 40, 400L));
            commit.commit(4, write.prepareCommit(false, 4));
        }

        long snapshotIdBefore = table.snapshotManager().latestSnapshotId();
        table.mergeBranch(BRANCH_NAME, "main");
        long snapshotIdAfter = table.snapshotManager().latestSnapshotId();

        // Second merge should be no-op (branch file already in target)
        assertThat(snapshotIdAfter).isEqualTo(snapshotIdBefore);

        assertRowIdRangesNonOverlapping(table);
        Snapshot finalSnapshot = table.snapshotManager().latestSnapshot();
        assertThat(finalSnapshot.nextRowId()).isEqualTo(6L);
    }

    private void assertRowIdRangesNonOverlapping(FileStoreTable table) {
        ManifestList manifestList = table.store().manifestListFactory().create();
        ManifestFile manifestFile = table.store().manifestFileFactory().create();
        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        Map<FileEntry.Identifier, ManifestEntry> files = new LinkedHashMap<>();
        FileEntry.mergeEntries(manifestFile, manifestList.readDataManifests(snapshot), files, null);

        List<long[]> ranges = new ArrayList<>();
        for (ManifestEntry entry : files.values()) {
            if (entry.file().firstRowId() != null) {
                long start = entry.file().firstRowId();
                long end = start + entry.file().rowCount() - 1;
                ranges.add(new long[] {start, end});
            }
        }
        ranges.sort(Comparator.comparingLong(r -> r[0]));
        for (int i = 1; i < ranges.size(); i++) {
            assertTrue(
                    ranges.get(i)[0] > ranges.get(i - 1)[1],
                    String.format(
                            "Row-id ranges overlap: [%d, %d] and [%d, %d]",
                            ranges.get(i - 1)[0],
                            ranges.get(i - 1)[1],
                            ranges.get(i)[0],
                            ranges.get(i)[1]));
        }
    }

    @Test
    public void testMergeBranchSameBranch() throws Exception {
        FileStoreTable table = createFileStoreTable();

        assertThatThrownBy(() -> table.mergeBranch(BRANCH_NAME, BRANCH_NAME))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Cannot merge branch 'branch1' into itself."));
    }

    @Test
    public void testMergeBranchSamePartition() throws Exception {
        FileStoreTable table = createBranchMergeTable();

        // Write data to main (partition pt=0)
        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(0, 0, 0L));
            commit.commit(0, write.prepareCommit(false, 1));
        }

        // Create branch from tag
        table.createTag("tag1", 1);
        table.createBranch(BRANCH_NAME, "tag1");
        FileStoreTable tableBranch = table.switchToBranch(BRANCH_NAME);

        // Write to branch with same partition pt=0
        try (StreamTableWrite write = tableBranch.newWrite(commitUser);
                StreamTableCommit commit = tableBranch.newCommit(commitUser)) {
            write.write(rowData(0, 10, 100L));
            commit.commit(1, write.prepareCommit(false, 2));
        }

        // Merge branch into main
        table.mergeBranch(BRANCH_NAME, "main");

        // Both files coexist in the same partition
        assertThat(
                        getResult(
                                table.newRead(),
                                toSplits(table.newSnapshotReader().read().dataSplits()),
                                BATCH_ROW_TO_STRING))
                .containsExactlyInAnyOrder(
                        "0|0|0|binary|varbinary|mapKey:mapVal|multiset",
                        "0|10|100|binary|varbinary|mapKey:mapVal|multiset");
    }

    @Test
    public void testMergeBranchNonExistentBranch() throws Exception {
        FileStoreTable table = createFileStoreTable();

        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(0, 0, 0L));
            commit.commit(0, write.prepareCommit(false, 1));
        }

        assertThatThrownBy(() -> table.mergeBranch("nonexistent", "main"))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Branch 'nonexistent' doesn't exist."));
    }

    @Test
    public void testMergeBranchMultiBucket() throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        conf -> {
                            conf.set(CoreOptions.BUCKET, 2);
                        });

        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(0, 0, 0L));
            write.write(rowData(0, 1, 1L));
            commit.commit(0, write.prepareCommit(false, 1));
        }

        table.createTag("tag1", 1);
        table.createBranch(BRANCH_NAME, "tag1");
        FileStoreTable tableBranch = table.switchToBranch(BRANCH_NAME);

        try (StreamTableWrite write = tableBranch.newWrite(commitUser);
                StreamTableCommit commit = tableBranch.newCommit(commitUser)) {
            write.write(rowData(1, 10, 100L));
            write.write(rowData(1, 11, 110L));
            commit.commit(1, write.prepareCommit(false, 2));
        }

        table.mergeBranch(BRANCH_NAME, "main");

        assertThat(
                        getResult(
                                table.newRead(),
                                toSplits(table.newSnapshotReader().read().dataSplits()),
                                BATCH_ROW_TO_STRING))
                .containsExactlyInAnyOrder(
                        "0|0|0|binary|varbinary|mapKey:mapVal|multiset",
                        "0|1|1|binary|varbinary|mapKey:mapVal|multiset",
                        "1|10|100|binary|varbinary|mapKey:mapVal|multiset",
                        "1|11|110|binary|varbinary|mapKey:mapVal|multiset");
    }

    @Test
    public void testMergeBranchNonExistentTargetBranch() throws Exception {
        FileStoreTable table = createFileStoreTable();

        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(0, 0, 0L));
            commit.commit(0, write.prepareCommit(false, 1));
        }

        table.createBranch(BRANCH_NAME);

        assertThatThrownBy(() -> table.mergeBranch(BRANCH_NAME, "nonexistent"))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Branch 'nonexistent' doesn't exist."));
    }

    @Test
    public void testMergeBranchBetweenNonMainBranches() throws Exception {
        FileStoreTable table = createBranchMergeTable();

        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(0, 0, 0L));
            commit.commit(0, write.prepareCommit(false, 1));
        }

        // Create two branches from tag so they share the same base data
        table.createTag("tag1", 1);
        table.createBranch("branchA", "tag1");
        table.createBranch("branchB", "tag1");
        FileStoreTable tableA = table.switchToBranch("branchA");
        FileStoreTable tableB = table.switchToBranch("branchB");

        // Write to branchA
        try (StreamTableWrite write = tableA.newWrite(commitUser);
                StreamTableCommit commit = tableA.newCommit(commitUser)) {
            write.write(rowData(1, 10, 100L));
            commit.commit(1, write.prepareCommit(false, 2));
        }

        // Write to branchB
        try (StreamTableWrite write = tableB.newWrite(commitUser);
                StreamTableCommit commit = tableB.newCommit(commitUser)) {
            write.write(rowData(2, 20, 200L));
            commit.commit(2, write.prepareCommit(false, 2));
        }

        // Merge branchA into branchB
        table.mergeBranch("branchA", "branchB");

        // Reload branchB table to see changes
        tableB = table.switchToBranch("branchB");
        assertThat(
                        getResult(
                                tableB.newRead(),
                                toSplits(tableB.newSnapshotReader().read().dataSplits()),
                                BATCH_ROW_TO_STRING))
                .containsExactlyInAnyOrder(
                        "0|0|0|binary|varbinary|mapKey:mapVal|multiset",
                        "1|10|100|binary|varbinary|mapKey:mapVal|multiset",
                        "2|20|200|binary|varbinary|mapKey:mapVal|multiset");
    }

    @Test
    public void testMergeBranchAfterSnapshotExpiration() throws Exception {
        FileStoreTable table =
                createBranchMergeTable(
                        conf -> {
                            conf.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN, 1);
                            conf.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX, 1);
                        });

        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(0, 0, 0L));
            commit.commit(0, write.prepareCommit(false, 1));
        }

        table.createTag("tag1", 1);
        table.createBranch(BRANCH_NAME, "tag1");
        FileStoreTable tableBranch = table.switchToBranch(BRANCH_NAME);

        try (StreamTableWrite write = tableBranch.newWrite(commitUser);
                StreamTableCommit commit = tableBranch.newCommit(commitUser)) {
            write.write(rowData(1, 10, 100L));
            commit.commit(1, write.prepareCommit(false, 2));
        }

        for (int i = 2; i < 5; i++) {
            try (StreamTableWrite write = tableBranch.newWrite(commitUser);
                    StreamTableCommit commit = tableBranch.newCommit(commitUser)) {
                write.write(rowData(i, i * 10, (long) i * 100));
                commit.commit(i, write.prepareCommit(false, i + 1));
            }
        }

        tableBranch.newExpireSnapshots().config(tableBranch.coreOptions().expireConfig()).expire();

        // After expiration, baseline snapshot is gone — merge should fail
        assertThatThrownBy(() -> table.mergeBranch(BRANCH_NAME, "main"))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Branch merge requires complete append-only snapshot history"));
    }

    @Test
    public void testMergeBranchRejectsNonAppendHistory() throws Exception {
        FileStoreTable table = createFileStoreTable();

        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(0, 0, 0L));
            commit.commit(0, write.prepareCommit(false, 1));
        }

        table.createTag("tag1", 1);
        table.createBranch(BRANCH_NAME, "tag1");
        FileStoreTable tableBranch = table.switchToBranch(BRANCH_NAME);

        // Write data to branch
        try (StreamTableWrite write = tableBranch.newWrite(commitUser);
                StreamTableCommit commit = tableBranch.newCommit(commitUser)) {
            write.write(rowData(0, 10, 100L));
            commit.commit(1, write.prepareCommit(false, 2));
        }

        // Perform INSERT OVERWRITE on branch — creates an OVERWRITE snapshot
        List<CommitMessage> commitMessages;
        try (BatchTableWrite write =
                tableBranch.newBatchWriteBuilder().withOverwrite().newWrite()) {
            write.write(rowData(0, 20, 200L));
            commitMessages = write.prepareCommit();
        }
        try (BatchTableCommit commit =
                tableBranch.newBatchWriteBuilder().withOverwrite().newCommit()) {
            commit.commit(commitMessages);
        }

        assertThatThrownBy(() -> table.mergeBranch(BRANCH_NAME, "main"))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Branch merge requires complete append-only snapshot history"));
    }

    @Test
    public void testMergeBranchRejectsTargetOverwrite() throws Exception {
        FileStoreTable table = createFileStoreTable();

        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(0, 0, 0L));
            commit.commit(0, write.prepareCommit(false, 1));
        }

        table.createTag("tag1", 1);
        table.createBranch(BRANCH_NAME, "tag1");
        FileStoreTable tableBranch = table.switchToBranch(BRANCH_NAME);

        try (StreamTableWrite write = tableBranch.newWrite(commitUser);
                StreamTableCommit commit = tableBranch.newCommit(commitUser)) {
            write.write(rowData(0, 10, 100L));
            commit.commit(1, write.prepareCommit(false, 2));
        }

        // INSERT OVERWRITE on target (main)
        List<CommitMessage> commitMessages;
        try (BatchTableWrite write = table.newBatchWriteBuilder().withOverwrite().newWrite()) {
            write.write(rowData(0, 20, 200L));
            commitMessages = write.prepareCommit();
        }
        try (BatchTableCommit commit = table.newBatchWriteBuilder().withOverwrite().newCommit()) {
            commit.commit(commitMessages);
        }

        assertThatThrownBy(() -> table.mergeBranch(BRANCH_NAME, "main"))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Branch merge requires complete append-only snapshot history"));
    }

    @Test
    public void testMergeBranchRejectsSourceOverwrite() throws Exception {
        FileStoreTable table = createFileStoreTable();

        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(0, 0, 0L));
            commit.commit(0, write.prepareCommit(false, 1));
        }

        table.createTag("tag1", 1);
        table.createBranch(BRANCH_NAME, "tag1");
        FileStoreTable tableBranch = table.switchToBranch(BRANCH_NAME);

        try (StreamTableWrite write = tableBranch.newWrite(commitUser);
                StreamTableCommit commit = tableBranch.newCommit(commitUser)) {
            write.write(rowData(0, 10, 100L));
            commit.commit(1, write.prepareCommit(false, 2));
        }

        // INSERT OVERWRITE on source (branch)
        List<CommitMessage> commitMessages;
        try (BatchTableWrite write =
                tableBranch.newBatchWriteBuilder().withOverwrite().newWrite()) {
            write.write(rowData(0, 20, 200L));
            commitMessages = write.prepareCommit();
        }
        try (BatchTableCommit commit =
                tableBranch.newBatchWriteBuilder().withOverwrite().newCommit()) {
            commit.commit(commitMessages);
        }

        assertThatThrownBy(() -> table.mergeBranch(BRANCH_NAME, "main"))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Branch merge requires complete append-only snapshot history"));
    }

    @Test
    public void testMergeBranchRejectsSourceExpiredSnapshots() throws Exception {
        FileStoreTable table =
                createBranchMergeTable(
                        conf -> {
                            conf.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN, 1);
                            conf.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX, 1);
                        });

        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(0, 0, 0L));
            commit.commit(0, write.prepareCommit(false, 1));
        }

        table.createTag("tag1", 1);
        table.createBranch(BRANCH_NAME, "tag1");
        FileStoreTable tableBranch = table.switchToBranch(BRANCH_NAME);

        for (int i = 1; i < 5; i++) {
            try (StreamTableWrite write = tableBranch.newWrite(commitUser);
                    StreamTableCommit commit = tableBranch.newCommit(commitUser)) {
                write.write(rowData(i, i * 10, (long) i * 100));
                commit.commit(i, write.prepareCommit(false, i + 1));
            }
        }

        tableBranch.newExpireSnapshots().config(tableBranch.coreOptions().expireConfig()).expire();

        assertThatThrownBy(() -> table.mergeBranch(BRANCH_NAME, "main"))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Branch merge requires complete append-only snapshot history"));
    }

    @Test
    public void testMergeBranchRejectsTargetExpiredSnapshots() throws Exception {
        FileStoreTable table =
                createBranchMergeTable(
                        conf -> {
                            conf.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN, 1);
                            conf.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX, 1);
                        });

        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(0, 0, 0L));
            commit.commit(0, write.prepareCommit(false, 1));
        }

        table.createTag("tag1", 1);
        table.createBranch(BRANCH_NAME, "tag1");
        FileStoreTable tableBranch = table.switchToBranch(BRANCH_NAME);

        try (StreamTableWrite write = tableBranch.newWrite(commitUser);
                StreamTableCommit commit = tableBranch.newCommit(commitUser)) {
            write.write(rowData(1, 10, 100L));
            commit.commit(1, write.prepareCommit(false, 2));
        }

        // Write multiple commits on main to allow expiration
        for (int i = 2; i < 5; i++) {
            try (StreamTableWrite write = table.newWrite(commitUser);
                    StreamTableCommit commit = table.newCommit(commitUser)) {
                write.write(rowData(i, i * 10, (long) i * 100));
                commit.commit(i, write.prepareCommit(false, i + 1));
            }
        }

        table.newExpireSnapshots().config(table.coreOptions().expireConfig()).expire();

        assertThatThrownBy(() -> table.mergeBranch(BRANCH_NAME, "main"))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Branch merge requires complete append-only snapshot history"));
    }

    @Test
    public void testMergeBranchFromTagSucceeds() throws Exception {
        FileStoreTable table = createFileStoreTable();

        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(0, 0, 0L));
            commit.commit(0, write.prepareCommit(false, 1));
        }

        table.createTag("tag1", 1);
        table.createBranch(BRANCH_NAME, "tag1");
        FileStoreTable tableBranch = table.switchToBranch(BRANCH_NAME);

        try (StreamTableWrite write = tableBranch.newWrite(commitUser);
                StreamTableCommit commit = tableBranch.newCommit(commitUser)) {
            write.write(rowData(1, 10, 100L));
            commit.commit(1, write.prepareCommit(false, 2));
        }

        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(2, 20, 200L));
            commit.commit(2, write.prepareCommit(false, 2));
        }

        table.mergeBranch(BRANCH_NAME, "main");

        assertThat(
                        getResult(
                                table.newRead(),
                                toSplits(table.newSnapshotReader().read().dataSplits()),
                                BATCH_ROW_TO_STRING))
                .containsExactlyInAnyOrder(
                        "0|0|0|binary|varbinary|mapKey:mapVal|multiset",
                        "1|10|100|binary|varbinary|mapKey:mapVal|multiset",
                        "2|20|200|binary|varbinary|mapKey:mapVal|multiset");
    }

    @Test
    public void testMergePlainBranchSucceedsWithCompleteHistory() throws Exception {
        FileStoreTable table = createBranchMergeTable();

        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(0, 0, 0L));
            commit.commit(0, write.prepareCommit(false, 1));
        }

        table.createBranch(BRANCH_NAME);
        FileStoreTable tableBranch = table.switchToBranch(BRANCH_NAME);

        try (StreamTableWrite write = tableBranch.newWrite(commitUser);
                StreamTableCommit commit = tableBranch.newCommit(commitUser)) {
            write.write(rowData(1, 10, 100L));
            commit.commit(1, write.prepareCommit(false, 2));
        }

        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(rowData(2, 20, 200L));
            commit.commit(2, write.prepareCommit(false, 2));
        }

        table.mergeBranch(BRANCH_NAME, "main");

        assertThat(
                        getResult(
                                table.newRead(),
                                toSplits(table.newSnapshotReader().read().dataSplits()),
                                BATCH_ROW_TO_STRING))
                .containsExactlyInAnyOrder(
                        "0|0|0|binary|varbinary|mapKey:mapVal|multiset",
                        "1|10|100|binary|varbinary|mapKey:mapVal|multiset",
                        "2|20|200|binary|varbinary|mapKey:mapVal|multiset");
    }
}
