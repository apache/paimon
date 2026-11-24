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

package org.apache.paimon.vector.index;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.CentroidIndexFile;
import org.apache.paimon.globalindex.CentroidIndexMeta;
import org.apache.paimon.index.DataEvolutionIndexSourceMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.source.RawVectorSearchSplit;
import org.apache.paimon.table.source.VectorScan;
import org.apache.paimon.table.source.VectorSearchBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.CompatibilityTestUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Opens Data Evolution tables and centroid manifests written by paimon-rust. */
class CentroidTableInteropTest {

    @TempDir java.nio.file.Path warehouse;
    private final FileIO fileIO = LocalFileIO.create();

    @BeforeEach
    void copyWarehouse() throws IOException {
        CompatibilityTestUtils.unzip("centroid-rust-warehouse.zip", warehouse.toString());
    }

    @Test
    void readsRustManifestAndSingleBitmapAcrossSchemaEvolution() throws Exception {
        Path location = tablePath("centroid_bitmap");
        FileStoreTable table = FileStoreTableFactory.create(fileIO, location);
        List<IndexFileMeta> files = indexFiles(table, "v", "ivf-flat");
        assertThat(files).hasSize(4);

        IndexFileMeta emptyRouter =
                files.stream().filter(CentroidIndexMeta::isEmptyRouter).findFirst().get();
        assertThat(ByteBuffer.wrap(descriptor(emptyRouter)).getInt(10)).isZero();
        assertThat(metric(emptyRouter)).isEqualTo("l2");
        assertThat(fileIO.exists(indexPath(table, emptyRouter))).isFalse();
        assertThat(files)
                .filteredOn(file -> CentroidIndexMeta.isRoutingModel(file) && file.fileSize() > 0)
                .singleElement()
                .satisfies(
                        file ->
                                assertThat(ByteBuffer.wrap(descriptor(file)).getInt(10))
                                        .isEqualTo(2));
        assertThat(files)
                .filteredOn(file -> descriptor(file)[9] == 'S')
                .singleElement()
                .satisfies(file -> assertThat(file.rowCount()).isEqualTo(4));
        IndexFileMeta bitmap =
                files.stream().filter(CentroidIndexMeta::isScalarIndex).findFirst().get();
        CentroidIndexFile bitmapFile =
                (CentroidIndexFile)
                        CentroidIndexMeta.file(
                                bitmap, table.store().pathFactory().globalIndexFileFactory());
        assertThat(bitmapFile.fieldId()).isEqualTo(2);
        assertThat(bitmapFile.metadata()).isNotEmpty();
        assertThat(bitmap.globalIndexMeta().extraFieldIds()).containsExactly(2);
        assertThat(DataEvolutionIndexSourceMeta.fromIndexFile(bitmap).sourceSchemaId())
                .isEqualTo(1)
                .isEqualTo(table.schema().id());

        int status = table.rowType().getFieldIndex("status");
        PredicateBuilder predicates = new PredicateBuilder(table.rowType());
        // With one candidate, post-filtering the unfiltered hit cannot pass either predicate.
        assertThat(search(table, "v", null, 1)).containsExactly(0);
        assertThat(search(table, "v", predicates.equal(status, 9), 1)).containsExactly(1);
        assertThat(search(table, "v", predicates.isNull(status), 1)).containsExactly(2);

        new FileSystemSchemaManager(fileIO, location)
                .commitChanges(SchemaChange.updateColumnType("status", DataTypes.BIGINT()));
        FileStoreTable evolved = FileStoreTableFactory.create(fileIO, location);
        assertThat(evolved.schema().id()).isGreaterThan(table.schema().id());
        // A compatible result after removing the old Bitmap proves the reader fell back.
        fileIO.delete(indexPath(table, bitmap), false);
        Predicate filter = new PredicateBuilder(evolved.rowType()).equal(status, 9L);
        assertThat(search(evolved, "v", filter, 8)).containsExactly(1);
    }

    @ParameterizedTest
    @CsvSource({"pq,ivf-pq,l2,0,0", "cos,ivf-flat,cosine,2,0", "ip,ivf-flat,inner_product,1,1"})
    void readsRustRouterMetricsAndIvfPqGeometry(
            String column, String type, String distance, int metricCode, long expected)
            throws IOException {
        FileStoreTable table = FileStoreTableFactory.create(fileIO, tablePath("centroid_metrics"));
        List<IndexFileMeta> files = indexFiles(table, column, type);
        IndexFileMeta router =
                files.stream().filter(CentroidIndexMeta::isRoutingModel).findFirst().get();
        assertThat(metric(router)).isEqualTo(distance);
        byte[] nativeRouter = payload(table, router);
        assertThat(new String(nativeRouter, 0, 8, StandardCharsets.US_ASCII)).isEqualTo("PVROUT01");
        assertThat(ByteBuffer.wrap(nativeRouter).order(ByteOrder.LITTLE_ENDIAN).getInt(16))
                .isEqualTo(metricCode);
        List<IndexFileMeta> populated =
                files.stream()
                        .filter(file -> descriptor(file)[9] == 'S' && file.rowCount() > 0)
                        .collect(Collectors.toList());
        assertThat(populated)
                .isNotEmpty()
                .allSatisfy(
                        shard -> {
                            if ("ivf-pq".equals(type)) {
                                assertThat(descriptor(shard)).hasSize(18);
                                assertThat(ByteBuffer.wrap(descriptor(shard)).getInt(14))
                                        .isEqualTo(64);
                            } else {
                                assertThat(descriptor(shard)).hasSize(14);
                            }
                        });
        assertThat(search(table, column, null, 8)).containsExactly(expected);
    }

    private Path tablePath(String table) {
        return new Path(warehouse.resolve("test.db").resolve(table).toUri());
    }

    @Test
    void rejectsCommitsThatOnlyDeleteCentroidShards() throws Exception {
        FileStoreTable table = FileStoreTableFactory.create(fileIO, tablePath("centroid_metrics"));
        List<IndexFileMeta> shards =
                indexFiles(table, "cos", "ivf-flat").stream()
                        .filter(file -> !CentroidIndexMeta.isRoutingModel(file))
                        .collect(Collectors.toList());
        assertThat(shards).isNotEmpty();
        CommitMessageImpl delete =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        null,
                        DataIncrement.deleteIndexIncrement(
                                Collections.singletonList(shards.get(0))),
                        CompactIncrement.emptyIncrement());
        try (TableCommitImpl commit = table.newCommit("delete-centroid-shards")) {
            assertThatThrownBy(() -> commit.commit(Collections.singletonList(delete)))
                    .hasStackTraceContaining("Cannot partially delete a centroid index generation");
        }
        assertThat(search(table, "cos", null, 1)).containsExactly(0);
    }

    private static List<IndexFileMeta> indexFiles(
            FileStoreTable table, String column, String type) {
        int id = table.rowType().getField(column).id();
        return table.store().newIndexFileHandler()
                .scan(table.snapshotManager().latestSnapshot(), type).stream()
                .map(IndexManifestEntry::indexFile)
                .filter(file -> file.globalIndexMeta().indexFieldId() == id)
                .collect(Collectors.toList());
    }

    private static byte[] descriptor(IndexFileMeta file) {
        return file.globalIndexMeta().indexMeta();
    }

    private static String metric(IndexFileMeta router) {
        byte[] bytes = descriptor(router);
        return new String(bytes, 14, bytes.length - 14, StandardCharsets.UTF_8);
    }

    private static Path indexPath(FileStoreTable table, IndexFileMeta file) {
        return table.store().pathFactory().globalIndexFileFactory().toPath(file);
    }

    private byte[] payload(FileStoreTable table, IndexFileMeta file) throws IOException {
        byte[] bytes = new byte[Math.toIntExact(file.fileSize())];
        try (DataInputStream input =
                new DataInputStream(fileIO.newInputStream(indexPath(table, file)))) {
            input.readFully(bytes);
        }
        return bytes;
    }

    private static long[] search(
            FileStoreTable table, String column, Predicate filter, int candidatesFactor) {
        VectorSearchBuilder query =
                table.newVectorSearchBuilder()
                        .withVectorColumn(column)
                        .withVector(new float[] {1, 0})
                        .withLimit(1)
                        .withOption("centroid.nprobe", "2")
                        .withOption("ivf.nprobe", "64")
                        .withOption(
                                "vector-index.post-filter.candidates-factor",
                                String.valueOf(candidatesFactor));
        if (filter != null) {
            query.withFilter(filter);
        }
        VectorScan.Plan plan = query.newVectorScan().scan();
        assertThat(plan.splits()).noneMatch(RawVectorSearchSplit.class::isInstance);
        return query.newVectorRead().read(plan).results().toArray(1);
    }
}
