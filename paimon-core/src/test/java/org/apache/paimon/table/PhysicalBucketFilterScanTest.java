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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestSidecar;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.PhysicalBucketPredicate;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.SegmentsCache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies physical bucket pruning through the public read API, including actual manifest I/O. */
class PhysicalBucketFilterScanTest {

    @TempDir java.nio.file.Path tempDir;

    private final CountingFileIO fileIO = new CountingFileIO();

    @ParameterizedTest
    @CsvSource({
        "false,false,false", "false,false,true", "false,true,false", "false,true,true",
        "true,false,false", "true,false,true", "true,true,false", "true,true,true"
    })
    void testManifestPruning(boolean pk, boolean cached, boolean sidecar) throws Exception {
        FileStoreTable table = createTable(pk, sidecar);
        for (int bucket = 0; bucket < 4; bucket++) {
            write(table, bucket, GenericRow.of(bucket * 10, 0), GenericRow.of(bucket * 10 + 1, 0));
        }
        List<ManifestFileMeta> manifests = manifests(table);
        assertThat(manifests).hasSize(4);
        assertThat(manifests).allMatch(m -> m.minBucket().equals(m.maxBucket()));
        assertThat(manifests).allMatch(m -> (ManifestSidecar.fileName(m) != null) == sidecar);

        ReadBuilder all = table.newReadBuilder();
        TableScan.Plan allPlan = plan(all, 4);
        assertThat(fileCount(allPlan)).isEqualTo(4);
        assertThat(readIds(all, allPlan)).containsExactlyInAnyOrder(0, 1, 10, 11, 20, 21, 30, 31);

        ReadBuilder ordinary = table.newReadBuilder().withBucketFilter(b -> b == 1);
        TableScan.Plan ordinaryPlan = plan(ordinary, sidecar ? 1 : 4);
        assertThat(fileIO.sidecarReads.get()).isEqualTo(sidecar ? 4 : 0);
        List<Integer> expected = readIds(ordinary, ordinaryPlan);
        assertThat(expected).containsExactlyInAnyOrder(10, 11);

        if (cached) {
            table.setManifestCache(cache());
            table.setManifestSidecarCache(cache());
        }
        ReadBuilder filtered =
                table.newReadBuilder().withBucketFilter(PhysicalBucketPredicate.equal(1));
        TableScan.Plan filteredPlan = plan(filtered, 1);
        assertThat(fileIO.sidecarReads.get()).isEqualTo(sidecar ? 1 : 0);
        assertThat(fileCount(filteredPlan)).isEqualTo(1);
        assertThat(filteredPlan.splits()).allMatch(split -> ((DataSplit) split).bucket() == 1);
        assertThat(readIds(filtered, filteredPlan)).containsExactlyInAnyOrderElementsOf(expected);
        assertThat(readIds(filtered, plan(filtered, cached ? 0 : 1)))
                .containsExactlyInAnyOrderElementsOf(expected);

        assertThat(
                        plan(
                                        table.newReadBuilder()
                                                .withBucketFilter(
                                                        PhysicalBucketPredicate.equal(99)),
                                        0)
                                .splits())
                .isEmpty();
        assertThat(
                        plan(
                                        table.newReadBuilder()
                                                .withBucketFilter(PhysicalBucketPredicate.none()),
                                        0)
                                .splits())
                .isEmpty();
        assertThat(
                        fileCount(
                                plan(
                                        table.newReadBuilder()
                                                .withBucketFilter(PhysicalBucketPredicate.all()),
                                        cached ? 3 : 4)))
                .isEqualTo(4);
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void testMixedManifestEntries(boolean pk, boolean sidecar) throws Exception {
        FileStoreTable table = createTable(pk, sidecar);
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (int bucket = 0; bucket < 4; bucket++) {
                write.write(GenericRow.of(bucket, 0), bucket);
            }
            commit.commit(write.prepareCommit());
        }
        assertThat(manifests(table)).hasSize(1);
        table.setManifestCache(cache());
        table.setManifestSidecarCache(cache());
        // The range [0,3] overlaps, so entry/sidecar filtering must remove the other buckets.
        for (int iteration = 0; iteration < 2; iteration++) {
            ReadBuilder read =
                    table.newReadBuilder().withBucketFilter(PhysicalBucketPredicate.in(1, 3));
            TableScan.Plan plan = plan(read, iteration == 0 ? 1 : 0);
            assertThat(fileCount(plan)).isEqualTo(2);
            assertThat(readIds(read, plan)).containsExactlyInAnyOrder(1, 3);
        }
    }

    @Test
    void testPrimaryKeyMergeAcrossFiles() throws Exception {
        FileStoreTable table = createTable(true, true);
        write(table, 1, GenericRow.of(10, 0));
        write(table, 1, GenericRow.of(10, 0), GenericRow.of(11, 0));
        write(table, 2, GenericRow.of(20, 0));
        ReadBuilder read =
                table.newReadBuilder().withBucketFilter(PhysicalBucketPredicate.equal(1));
        TableScan.Plan plan = plan(read, 2);
        assertThat(fileCount(plan)).isEqualTo(2);
        assertThat(readIds(read, plan)).containsExactlyInAnyOrder(10, 11);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testDeletedAndReplacedFiles(boolean pk) throws Exception {
        FileStoreTable table = createTable(pk, true);
        write(table, 1, GenericRow.of(10, 0), GenericRow.of(11, 1));
        write(table, 1, GenericRow.of(12, 0), GenericRow.of(13, 1));
        // Bucketed append full compaction requires at least three input files.
        write(table, 1, GenericRow.of(14, 0), GenericRow.of(15, 1));
        write(table, 2, GenericRow.of(20, 1));
        ReadBuilder read =
                table.newReadBuilder().withBucketFilter(PhysicalBucketPredicate.equal(1));
        List<String> originalFiles = fileNames(read.newScan().plan());

        BatchWriteBuilder builder =
                table.copy(Collections.singletonMap("write-only", "false")).newBatchWriteBuilder();
        BinaryRow partition =
                new InternalRowSerializer(RowType.of(DataTypes.INT()))
                        .toBinaryRow(GenericRow.of(1))
                        .copy();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.compact(partition, 1, true);
            commit.commit(write.prepareCommit());
        }
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.truncatePartitions(
                    Collections.singletonList(Collections.singletonMap("p", "0")));
        }
        TableScan.Plan plan = read.newScan().plan();
        assertThat(fileNames(plan)).doesNotContainAnyElementsOf(originalFiles);
        assertThat(readIds(read, plan)).containsExactlyInAnyOrder(11, 13, 15);
        ReadBuilder reference = table.newReadBuilder().withBucketFilter(b -> b == 1);
        assertThat(readIds(reference, reference.newScan().plan()))
                .containsExactlyInAnyOrder(11, 13, 15);
    }

    @Test
    void testHistoricalBucketCounts() throws Exception {
        FileStoreTable table = createTable(false, false);
        write(table, 3, GenericRow.of(30, 0));
        // Changing the count for a different partition must not truncate the physical ID domain.
        new FileSystemSchemaManager(fileIO, table.location())
                .commitChanges(Collections.singletonList(SchemaChange.setOption("bucket", "2")));
        table = FileStoreTableFactory.create(fileIO, table.location());
        write(table, 1, GenericRow.of(11, 1));
        assertThat(manifests(table))
                .extracting(ManifestFileMeta::totalBuckets)
                .containsExactlyInAnyOrder(4, 2);
        ReadBuilder read =
                table.newReadBuilder().withBucketFilter(PhysicalBucketPredicate.equal(3));
        assertThat(readIds(read, plan(read, 1))).containsExactly(30);
    }

    @Test
    void testReadBuilderContracts() throws Exception {
        FileStoreTable table = createTable(false, false);
        write(table, 1, GenericRow.of(10, 0), GenericRow.of(11, 1));
        write(table, 2, GenericRow.of(20, 0));
        ReadBuilder read =
                table.newReadBuilder()
                        .withBucketFilter(PhysicalBucketPredicate.none())
                        .withBucketFilter(PhysicalBucketPredicate.equal(1))
                        .withPartitionFilter(Collections.singletonMap("p", "1"))
                        .withBucket(1);
        ReadBuilder restored =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(read), getClass().getClassLoader());
        assertThat(readIds(restored, restored.newScan().plan())).containsExactly(11);
        assertThat(read.withBucket(2).newScan().plan().splits()).isEmpty();
        assertThatThrownBy(
                        () ->
                                table.newReadBuilder()
                                        .withBucketFilter(PhysicalBucketPredicate.all())
                                        .withShard(0, 2)
                                        .newScan())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Bucket filter and shard");
        assertThatThrownBy(
                        () ->
                                table.newReadBuilder()
                                        .withShard(0, 2)
                                        .withBucketFilter(PhysicalBucketPredicate.all())
                                        .newScan())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Bucket filter and shard");
    }

    private FileStoreTable createTable(boolean pk, boolean sidecar) throws Exception {
        Path path = new Path(tempDir.toUri());
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("p", DataTypes.INT())
                        .partitionKeys("p")
                        .option("bucket", "4")
                        .option("bucket-key", "id")
                        .option("file.format", "avro")
                        .option("write-only", "true")
                        .option("manifest.merge-min-count", "1000")
                        .option("manifest-sort.enabled", "false")
                        .option("manifest.sidecar.enabled", Boolean.toString(sidecar))
                        .option("scan.manifest.parallelism", "1");
        if (pk) {
            schema.primaryKey("p", "id");
        }
        new FileSystemSchemaManager(fileIO, path).createTable(schema.build());
        return FileStoreTableFactory.create(fileIO, path);
    }

    private void write(FileStoreTable table, int bucket, GenericRow... rows) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (GenericRow row : rows) {
                write.write(row, bucket);
            }
            commit.commit(write.prepareCommit());
        }
    }

    private List<ManifestFileMeta> manifests(FileStoreTable table) {
        return table.store()
                .manifestListFactory()
                .create()
                .readDataManifests(table.latestSnapshot().get());
    }

    private TableScan.Plan plan(ReadBuilder read, int expectedManifestReads) {
        fileIO.manifestReads.set(0);
        fileIO.sidecarReads.set(0);
        TableScan.Plan plan = read.newScan().plan();
        assertThat(fileIO.manifestReads.get())
                .as("manifest files opened")
                .isEqualTo(expectedManifestReads);
        return plan;
    }

    private static List<String> fileNames(TableScan.Plan plan) {
        return plan.splits().stream()
                .flatMap(split -> ((DataSplit) split).dataFiles().stream())
                .map(file -> file.fileName())
                .collect(Collectors.toList());
    }

    private static int fileCount(TableScan.Plan plan) {
        return fileNames(plan).size();
    }

    private static List<Integer> readIds(ReadBuilder read, TableScan.Plan plan) throws Exception {
        List<Integer> ids = new ArrayList<>();
        try (CloseableIterator<InternalRow> rows =
                read.newRead().createReader(plan).toCloseableIterator()) {
            while (rows.hasNext()) {
                ids.add(rows.next().getInt(0));
            }
        }
        return ids;
    }

    private static SegmentsCache<Path> cache() {
        return new SegmentsCache<>(1024, MemorySize.ofMebiBytes(16), Long.MAX_VALUE, null, false);
    }

    private static class CountingFileIO extends LocalFileIO {
        private static final long serialVersionUID = 1L;
        private final AtomicInteger manifestReads = new AtomicInteger();
        private final AtomicInteger sidecarReads = new AtomicInteger();

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            String name = path.getName();
            if (name.endsWith(ManifestSidecar.SUFFIX)) {
                sidecarReads.incrementAndGet();
            }
            if (name.startsWith("manifest-")
                    && !name.startsWith("manifest-list-")
                    && !name.endsWith(ManifestSidecar.SUFFIX)) {
                manifestReads.incrementAndGet();
            }
            return super.newInputStream(path);
        }
    }
}
