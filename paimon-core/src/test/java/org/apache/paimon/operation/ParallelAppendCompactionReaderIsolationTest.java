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

import org.apache.paimon.AppendOnlyFileStore;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.IOExceptionSupplier;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for parallel append compaction: each worker must use isolated compaction readers so
 * nested cast state is not shared across buckets.
 */
public class ParallelAppendCompactionReaderIsolationTest {

    @TempDir java.nio.file.Path tempDir;

    @ParameterizedTest
    @ValueSource(ints = {2, -1})
    public void testParallelCompactRewriteAfterNestedSchemaEvolution(int compactionTaskThreads)
            throws Exception {
        Path warehouse = new Path(tempDir.toString());
        Catalog catalog = new FileSystemCatalog(LocalFileIO.create(), warehouse);
        Identifier identifier = Identifier.create("default", "append_parallel_compact");
        catalog.createDatabase("default", true);

        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("bk", DataTypes.INT())
                        .column("marker", DataTypes.INT())
                        .column(
                                "payload",
                                DataTypes.ROW(DataTypes.FIELD(0, "val", DataTypes.INT())))
                        .partitionKeys("pt")
                        .option(BUCKET.key(), "2")
                        .option("bucket-key", "bk")
                        .option("file.format", "parquet")
                        .option(CoreOptions.COMPACTION_TASK_THREADS.key(), "1")
                        .option("target-file-size", "256 b")
                        .option("write-buffer-size", "256 b")
                        .build();
        catalog.createTable(identifier, schema, false);

        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
        BinaryRow partition = partition(0);
        String commitUser = "user-1";

        try (StreamTableCommit commit = table.newStreamWriteBuilder().newCommit()) {
            BaseAppendFileStoreWrite write =
                    (BaseAppendFileStoreWrite) table.store().newWrite(commitUser);
            for (int i = 0; i < 24; i++) {
                int bucket = i % 2;
                int marker = bucket == 0 ? 100 : 200;
                write.write(
                        partition,
                        bucket,
                        GenericRow.of(0, bucket, marker, GenericRow.of(i)));
                commit.commit(i, write.prepareCommit(false, i));
            }
            write.close();
        }

        Path tablePath = new Path(warehouse, "default.db/append_parallel_compact");
        SchemaManager schemaManager =
                new FileSystemSchemaManager(LocalFileIO.create(), tablePath);
        schemaManager.commitChanges(
                SchemaChange.updateColumnType(
                        new String[] {"payload", "val"}, DataTypes.BIGINT(), false));

        table =
                (FileStoreTable)
                        catalog.getTable(identifier)
                                .copy(
                                        Collections.singletonMap(
                                                CoreOptions.COMPACTION_TASK_THREADS.key(),
                                                String.valueOf(compactionTaskThreads)));

        List<DataFileMeta> bucket0Files = dataFiles(table, partition, 0);
        List<DataFileMeta> bucket1Files = dataFiles(table, partition, 1);
        assertThat(bucket0Files).isNotEmpty();
        assertThat(bucket1Files).isNotEmpty();

        BaseAppendFileStoreWrite write =
                (BaseAppendFileStoreWrite) table.store().newWrite(commitUser);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<List<DataFileMeta>> bucket0Future =
                    pool.submit(() -> write.compactRewrite(partition, 0, null, bucket0Files));
            Future<List<DataFileMeta>> bucket1Future =
                    pool.submit(() -> write.compactRewrite(partition, 1, null, bucket1Files));
            assertRowsHaveMarker(
                    table, partition, 0, bucket0Future.get(), 100);
            assertRowsHaveMarker(
                    table, partition, 1, bucket1Future.get(), 200);
        } finally {
            pool.shutdownNow();
            write.close();
        }
    }

    private static List<DataFileMeta> dataFiles(
            FileStoreTable table, BinaryRow partition, int bucket) {
        List<DataFileMeta> files = new ArrayList<>();
        for (Split split : table.newScan().plan().splits()) {
            DataSplit dataSplit = (DataSplit) split;
            if (dataSplit.bucket() == bucket && dataSplit.partition().equals(partition)) {
                files.addAll(dataSplit.dataFiles());
            }
        }
        return files;
    }

    private static void assertRowsHaveMarker(
            FileStoreTable table,
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> files,
            int expectedMarker)
            throws Exception {
        assertThat(files).isNotEmpty();
        RawFileSplitRead read = ((AppendOnlyFileStore) table.store()).newRead();
        @Nullable
        Map<String, IOExceptionSupplier<DeletionVector>> dvFactories = null;
        try (RecordReaderIterator<InternalRow> iterator =
                new RecordReaderIterator<>(
                        read.createReader(partition, bucket, files, dvFactories))) {
            List<Integer> markers = new ArrayList<>();
            while (iterator.hasNext()) {
                markers.add(iterator.next().getInt(2));
            }
            assertThat(markers).isNotEmpty();
            assertThat(markers.stream().distinct().collect(Collectors.toList()))
                    .containsExactly(expectedMarker);
        }
    }

    private static BinaryRow partition(int pt) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, pt);
        writer.complete();
        return row;
    }
}
