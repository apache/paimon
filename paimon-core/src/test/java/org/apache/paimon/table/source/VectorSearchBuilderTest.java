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

package org.apache.paimon.table.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.globalindex.GlobalIndexBuilderUtils;
import org.apache.paimon.globalindex.GlobalIndexParallelWriter;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexSingletonWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.btree.BTreeGlobalIndexerFactory;
import org.apache.paimon.globalindex.testvector.TestVectorGlobalIndexerFactory;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link VectorSearchBuilder} using test-only brute-force vector index. */
public class VectorSearchBuilderTest extends TableTestBase {

    private static final String VECTOR_FIELD_NAME = "vec";
    private static final int DIMENSION = 2;

    @Override
    protected Schema schemaDefault() {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column(VECTOR_FIELD_NAME, new ArrayType(DataTypes.FLOAT()))
                .option(CoreOptions.BUCKET.key(), "-1")
                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                .option("test.vector.dimension", String.valueOf(DIMENSION))
                .option("test.vector.metric", "l2")
                .build();
    }

    @Test
    public void testVectorSearchEndToEnd() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        float[][] vectors = {
            {1.0f, 0.0f},
            {0.95f, 0.1f},
            {0.1f, 0.95f},
            {0.98f, 0.05f},
            {0.0f, 1.0f},
            {0.05f, 0.98f}
        };

        writeVectors(table, vectors);
        buildAndCommitIndex(table, vectors);

        // Query vector close to (1.0, 0.0) - should return rows 0,1,3
        float[] queryVector = {0.85f, 0.15f};
        GlobalIndexResult result =
                table.newVectorSearchBuilder()
                        .withVector(queryVector)
                        .withLimit(3)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .executeLocal();

        assertThat(result).isInstanceOf(ScoredGlobalIndexResult.class);
        assertThat(result.results().isEmpty()).isFalse();

        // Read using the search result
        ReadBuilder readBuilder = table.newReadBuilder();
        List<Integer> ids = new ArrayList<>();
        TableScan.Plan plan = readBuilder.newScan().withGlobalIndexResult(result).plan();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }

        assertThat(ids).isNotEmpty();
        assertThat(ids.size()).isLessThanOrEqualTo(3);
        // Row 0 (1.0, 0.0) should be the closest to query (0.85, 0.15)
        assertThat(ids).contains(0);
    }

    @Test
    public void testVectorSearchWithCosineMetric() throws Exception {
        // Create a table with cosine metric
        catalog.createTable(
                identifier("cosine_table"),
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column(VECTOR_FIELD_NAME, new ArrayType(DataTypes.FLOAT()))
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option("test.vector.dimension", String.valueOf(DIMENSION))
                        .option("test.vector.metric", "cosine")
                        .build(),
                false);
        FileStoreTable table = getTable(identifier("cosine_table"));

        float[][] vectors = {
            {1.0f, 0.0f},
            {0.707f, 0.707f},
            {0.0f, 1.0f},
        };

        writeVectors(table, vectors);
        buildAndCommitIndex(table, vectors);

        // Query along x-axis: closest should be (1,0), then (0.707,0.707), then (0,1)
        float[] queryVector = {1.0f, 0.0f};
        GlobalIndexResult result =
                table.newVectorSearchBuilder()
                        .withVector(queryVector)
                        .withLimit(2)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .executeLocal();

        assertThat(result).isInstanceOf(ScoredGlobalIndexResult.class);

        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().withGlobalIndexResult(result).plan();
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }

        assertThat(ids).hasSize(2);
        // Row 0 (1,0) has cosine similarity = 1.0, row 1 (0.707,0.707) ~ 0.707
        assertThat(ids).contains(0, 1);
    }

    @Test
    public void testVectorSearchEmptyResult() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        // Write data but no index - should return empty result
        float[][] vectors = {{1.0f, 0.0f}, {0.0f, 1.0f}};
        writeVectors(table, vectors);

        GlobalIndexResult result =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {1.0f, 0.0f})
                        .withLimit(1)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .executeLocal();

        assertThat(result.results().isEmpty()).isTrue();
    }

    @Test
    public void testVectorSearchTopKLimit() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        float[][] vectors = new float[20][];
        for (int i = 0; i < 20; i++) {
            vectors[i] = new float[] {(float) Math.cos(i * 0.3), (float) Math.sin(i * 0.3)};
        }

        writeVectors(table, vectors);
        buildAndCommitIndex(table, vectors);

        // Search with limit=5
        GlobalIndexResult result =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {1.0f, 0.0f})
                        .withLimit(5)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .executeLocal();

        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().withGlobalIndexResult(result).plan();
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }

        assertThat(ids.size()).isLessThanOrEqualTo(5);
    }

    @Test
    public void testVectorSearchWithMultipleIndexFiles() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        float[][] allVectors = {
            {1.0f, 0.0f}, // row 0 - close to (1,0)
            {0.95f, 0.1f}, // row 1 - close to (1,0)
            {0.1f, 0.95f}, // row 2 - far from (1,0)
            {0.98f, 0.05f}, // row 3 - close to (1,0)
            {0.0f, 1.0f}, // row 4 - far from (1,0)
            {0.05f, 0.98f} // row 5 - far from (1,0)
        };

        writeVectors(table, allVectors);

        // Build two separate index files covering different row ranges
        buildAndCommitMultipleIndexFiles(table, allVectors);

        // Query vector close to (1.0, 0.0) - results should span across both index files
        float[] queryVector = {0.85f, 0.15f};
        GlobalIndexResult result =
                table.newVectorSearchBuilder()
                        .withVector(queryVector)
                        .withLimit(3)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .executeLocal();

        assertThat(result).isInstanceOf(ScoredGlobalIndexResult.class);
        assertThat(result.results().isEmpty()).isFalse();

        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().withGlobalIndexResult(result).plan();
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }

        assertThat(ids).isNotEmpty();
        assertThat(ids.size()).isLessThanOrEqualTo(3);
        // Row 0 (1.0,0.0), Row 1 (0.95,0.1), Row 3 (0.98,0.05) are closest to query
        // Row 0 is in the first index file, Row 3 is in the second index file
        assertThat(ids).contains(0);
        assertThat(ids).containsAnyOf(1, 3);
    }

    @Test
    public void testVectorSearchWithPartitionFilter() throws Exception {
        // Create a partitioned table
        catalog.createTable(
                identifier("partitioned_table"),
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("id", DataTypes.INT())
                        .column(VECTOR_FIELD_NAME, new ArrayType(DataTypes.FLOAT()))
                        .partitionKeys("pt")
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option("test.vector.dimension", String.valueOf(DIMENSION))
                        .option("test.vector.metric", "l2")
                        .build(),
                false);
        FileStoreTable table = getTable(identifier("partitioned_table"));

        // Partition 1: vectors close to (1,0)
        float[][] pt1Vectors = {{1.0f, 0.0f}, {0.95f, 0.1f}, {0.98f, 0.05f}};
        // Partition 2: vectors close to (0,1)
        float[][] pt2Vectors = {{0.0f, 1.0f}, {0.1f, 0.95f}, {0.05f, 0.98f}};

        writePartitionedVectors(table, 1, pt1Vectors);
        writePartitionedVectors(table, 2, pt2Vectors);

        RowType partitionType = RowType.of(DataTypes.INT());
        InternalRowSerializer serializer = new InternalRowSerializer(partitionType);
        BinaryRow partition1 = serializer.toBinaryRow(GenericRow.of(1)).copy();
        BinaryRow partition2 = serializer.toBinaryRow(GenericRow.of(2)).copy();

        // Build and commit indexes with non-overlapping row ranges
        buildAndCommitPartitionedIndex(table, pt1Vectors, partition1, new Range(0, 2));
        buildAndCommitPartitionedIndex(table, pt2Vectors, partition2, new Range(3, 5));

        float[] queryVector = {0.9f, 0.1f};

        // Search with partition filter for partition 1 only
        PartitionPredicate partFilter1 =
                PartitionPredicate.fromMultiple(
                        partitionType, Collections.singletonList(partition1));
        GlobalIndexResult result1 =
                table.newVectorSearchBuilder()
                        .withPartitionFilter(partFilter1)
                        .withVector(queryVector)
                        .withLimit(3)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .executeLocal();

        assertThat(result1).isInstanceOf(ScoredGlobalIndexResult.class);
        assertThat(result1.results().isEmpty()).isFalse();
        // Row IDs should be within partition 1's row range [0, 2]
        for (long rowId : result1.results()) {
            assertThat(rowId).isBetween(0L, 2L);
        }

        // Search with partition filter for partition 2 only
        PartitionPredicate partFilter2 =
                PartitionPredicate.fromMultiple(
                        partitionType, Collections.singletonList(partition2));
        GlobalIndexResult result2 =
                table.newVectorSearchBuilder()
                        .withPartitionFilter(partFilter2)
                        .withVector(queryVector)
                        .withLimit(3)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .executeLocal();

        assertThat(result2).isInstanceOf(ScoredGlobalIndexResult.class);
        assertThat(result2.results().isEmpty()).isFalse();
        // Row IDs should be within partition 2's row range [3, 5]
        for (long rowId : result2.results()) {
            assertThat(rowId).isBetween(3L, 5L);
        }

        // Search without partition filter - returns results from both partitions
        GlobalIndexResult resultAll =
                table.newVectorSearchBuilder()
                        .withVector(queryVector)
                        .withLimit(6)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .executeLocal();

        assertThat(resultAll.results().getIntCardinality())
                .isEqualTo(
                        result1.results().getIntCardinality()
                                + result2.results().getIntCardinality());
    }

    @Test
    public void testScanPartialRangeIntersection() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        // Write 10 rows
        float[][] allVectors = new float[10][];
        for (int i = 0; i < 10; i++) {
            allVectors[i] = new float[] {(float) Math.cos(i * 0.3), (float) Math.sin(i * 0.3)};
        }
        writeVectors(table, allVectors);

        // Build ONE vector index covering full range [0,9]
        buildAndCommitVectorIndex(table, allVectors, new Range(0, 9));

        // Build ONE btree index covering partial range [3,7]
        buildAndCommitBTreeIndex(table, new int[] {3, 4, 5, 6, 7}, new Range(3, 7));

        // VectorScanImpl should attach scalar index because [3,7] intersects [0,9]
        Predicate idFilter = new PredicateBuilder(table.rowType()).greaterOrEqual(0, 5);
        VectorScan.Plan plan =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {1.0f, 0.0f})
                        .withLimit(5)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .withFilter(idFilter)
                        .newVectorScan()
                        .scan();

        assertThat(plan.splits()).hasSize(1);
        VectorSearchSplit split = plan.splits().get(0);
        assertThat(split.rowRangeStart()).isEqualTo(0);
        assertThat(split.rowRangeEnd()).isEqualTo(9);
        assertThat(split.vectorIndexFiles()).isNotEmpty();
        // Scalar index [3,7] intersects vector range [0,9] → attached
        assertThat(split.scalarIndexFiles()).isNotEmpty();

        // Read with pre-filter: id >= 5, btree covers [3,7] so rows 5,6,7 from btree
        GlobalIndexResult result =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {1.0f, 0.0f})
                        .withLimit(5)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .withFilter(idFilter)
                        .newVectorRead()
                        .read(plan);

        assertThat(result).isInstanceOf(ScoredGlobalIndexResult.class);
        assertThat(result.results().isEmpty()).isFalse();
        // Pre-filter restricts to rows matching id >= 5 from btree [3,7]
        for (long rowId : result.results()) {
            assertThat(rowId).isBetween(5L, 7L);
        }
    }

    @Test
    public void testPreFilterMatchesZeroRows() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        float[][] vectors = {
            {1.0f, 0.0f},
            {0.95f, 0.1f},
            {0.0f, 1.0f},
            {0.1f, 0.95f}
        };
        writeVectors(table, vectors);

        Range range = new Range(0, 3);
        buildAndCommitVectorIndex(table, vectors, range);
        buildAndCommitBTreeIndex(table, new int[] {0, 1, 2, 3}, range);

        // Filter id > 100: btree covers ids 0-3, so preFilter matches zero rows
        Predicate impossibleFilter = new PredicateBuilder(table.rowType()).greaterThan(0, 100);
        VectorSearchBuilder searchBuilder =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {1.0f, 0.0f})
                        .withLimit(4)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .withFilter(impossibleFilter);

        VectorScan.Plan plan = searchBuilder.newVectorScan().scan();
        assertThat(plan.splits()).hasSize(1);
        // Scalar index is attached since field matches filter
        assertThat(plan.splits().get(0).scalarIndexFiles()).isNotEmpty();

        // Read: preFilter returns empty bitmap → vector search returns no results
        GlobalIndexResult result = searchBuilder.newVectorRead().read(plan);
        assertThat(result.results().isEmpty()).isTrue();
    }

    @Test
    public void testVectorSearchSplitSerialization() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        float[][] vectors = {{1.0f, 0.0f}, {0.0f, 1.0f}};
        writeVectors(table, vectors);

        Range range = new Range(0, 1);
        buildAndCommitVectorIndex(table, vectors, range);
        buildAndCommitBTreeIndex(table, new int[] {0, 1}, range);

        Predicate filter = new PredicateBuilder(table.rowType()).greaterOrEqual(0, 0);
        VectorScan.Plan plan =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {1.0f, 0.0f})
                        .withLimit(2)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .withFilter(filter)
                        .newVectorScan()
                        .scan();

        assertThat(plan.splits()).hasSize(1);
        VectorSearchSplit original = plan.splits().get(0);

        // Serialize
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(original);
        }

        // Deserialize
        VectorSearchSplit deserialized;
        try (ObjectInputStream in =
                new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
            deserialized = (VectorSearchSplit) in.readObject();
        }

        // Verify all fields match
        assertThat(deserialized.rowRangeStart()).isEqualTo(original.rowRangeStart());
        assertThat(deserialized.rowRangeEnd()).isEqualTo(original.rowRangeEnd());
        assertThat(deserialized.vectorIndexFiles()).hasSize(original.vectorIndexFiles().size());
        assertThat(deserialized.scalarIndexFiles()).hasSize(original.scalarIndexFiles().size());
        for (int i = 0; i < original.vectorIndexFiles().size(); i++) {
            assertThat(deserialized.vectorIndexFiles().get(i).fileName())
                    .isEqualTo(original.vectorIndexFiles().get(i).fileName());
        }
        for (int i = 0; i < original.scalarIndexFiles().size(); i++) {
            assertThat(deserialized.scalarIndexFiles().get(i).fileName())
                    .isEqualTo(original.scalarIndexFiles().get(i).fileName());
        }
    }

    // ====================== Helper methods ======================

    private void writeVectors(FileStoreTable table, float[][] vectors) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (int i = 0; i < vectors.length; i++) {
                write.write(GenericRow.of(i, new GenericArray(vectors[i])));
            }
            commit.commit(write.prepareCommit());
        }
    }

    private void buildAndCommitIndex(FileStoreTable table, float[][] vectors) throws Exception {
        Options options = table.coreOptions().toConfiguration();
        DataField vectorField = table.rowType().getField(VECTOR_FIELD_NAME);

        GlobalIndexSingletonWriter writer =
                (GlobalIndexSingletonWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table,
                                TestVectorGlobalIndexerFactory.IDENTIFIER,
                                vectorField,
                                options);
        for (float[] vec : vectors) {
            writer.write(vec);
        }
        List<ResultEntry> entries = writer.finish();

        Range rowRange = new Range(0, vectors.length - 1);
        List<IndexFileMeta> indexFiles =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        rowRange,
                        vectorField.id(),
                        TestVectorGlobalIndexerFactory.IDENTIFIER,
                        entries);

        DataIncrement dataIncrement = DataIncrement.indexIncrement(indexFiles);
        CommitMessage message =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        null,
                        dataIncrement,
                        CompactIncrement.emptyIncrement());
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(Collections.singletonList(message));
        }
    }

    private void buildAndCommitMultipleIndexFiles(FileStoreTable table, float[][] vectors)
            throws Exception {
        Options options = table.coreOptions().toConfiguration();
        DataField vectorField = table.rowType().getField(VECTOR_FIELD_NAME);
        int mid = vectors.length / 2;

        // Build first index file covering rows [0, mid)
        GlobalIndexSingletonWriter writer1 =
                (GlobalIndexSingletonWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table,
                                TestVectorGlobalIndexerFactory.IDENTIFIER,
                                vectorField,
                                options);
        for (int i = 0; i < mid; i++) {
            writer1.write(vectors[i]);
        }
        List<ResultEntry> entries1 = writer1.finish();
        Range rowRange1 = new Range(0, mid - 1);
        List<IndexFileMeta> indexFiles1 =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        rowRange1,
                        vectorField.id(),
                        TestVectorGlobalIndexerFactory.IDENTIFIER,
                        entries1);

        // Build second index file covering rows [mid, end)
        GlobalIndexSingletonWriter writer2 =
                (GlobalIndexSingletonWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table,
                                TestVectorGlobalIndexerFactory.IDENTIFIER,
                                vectorField,
                                options);
        for (int i = mid; i < vectors.length; i++) {
            writer2.write(vectors[i]);
        }
        List<ResultEntry> entries2 = writer2.finish();
        Range rowRange2 = new Range(mid, vectors.length - 1);
        List<IndexFileMeta> indexFiles2 =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        rowRange2,
                        vectorField.id(),
                        TestVectorGlobalIndexerFactory.IDENTIFIER,
                        entries2);

        // Combine all index files and commit together
        List<IndexFileMeta> allIndexFiles = new ArrayList<>();
        allIndexFiles.addAll(indexFiles1);
        allIndexFiles.addAll(indexFiles2);

        DataIncrement dataIncrement = DataIncrement.indexIncrement(allIndexFiles);
        CommitMessage message =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        null,
                        dataIncrement,
                        CompactIncrement.emptyIncrement());
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(Collections.singletonList(message));
        }
    }

    private void writePartitionedVectors(FileStoreTable table, int partition, float[][] vectors)
            throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (int i = 0; i < vectors.length; i++) {
                write.write(GenericRow.of(partition, i, new GenericArray(vectors[i])));
            }
            commit.commit(write.prepareCommit());
        }
    }

    @Test
    public void testVectorSearchWithBTreePreFilter() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        // Write 10 rows: ids 0-9 with vectors
        // Rows 0-4: vectors near (1,0)
        // Rows 5-9: vectors near (0,1)
        float[][] allVectors = {
            {1.0f, 0.0f}, // row 0
            {0.95f, 0.1f}, // row 1
            {0.98f, 0.05f}, // row 2
            {0.9f, 0.15f}, // row 3
            {0.85f, 0.2f}, // row 4
            {0.0f, 1.0f}, // row 5
            {0.1f, 0.95f}, // row 6
            {0.05f, 0.98f}, // row 7
            {0.15f, 0.9f}, // row 8
            {0.2f, 0.85f} // row 9
        };
        writeVectors(table, allVectors);

        Range range1 = new Range(0, 4);
        Range range2 = new Range(5, 9);

        // Build two vector index files for each range
        buildAndCommitVectorIndex(
                table,
                new float[][] {
                    allVectors[0], allVectors[1], allVectors[2], allVectors[3], allVectors[4]
                },
                range1);
        buildAndCommitVectorIndex(
                table,
                new float[][] {
                    allVectors[5], allVectors[6], allVectors[7], allVectors[8], allVectors[9]
                },
                range2);

        // Build two btree indexes on 'id' field for each range
        buildAndCommitBTreeIndex(table, new int[] {0, 1, 2, 3, 4}, range1);
        buildAndCommitBTreeIndex(table, new int[] {5, 6, 7, 8, 9}, range2);

        // --- Test VectorScanImpl: verify splits contain scalar index files ---
        Predicate idFilter = new PredicateBuilder(table.rowType()).greaterOrEqual(0, 5);
        VectorSearchBuilder searchBuilder =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {0.1f, 0.9f})
                        .withLimit(5)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .withFilter(idFilter);

        VectorScan.Plan plan = searchBuilder.newVectorScan().scan();
        assertThat(plan.splits()).isNotEmpty();
        // Every split should have vector index files
        for (VectorSearchSplit split : plan.splits()) {
            assertThat(split.vectorIndexFiles()).isNotEmpty();
        }
        // At least one split should have scalar (btree) index files
        long scalarCount =
                plan.splits().stream().filter(s -> !s.scalarIndexFiles().isEmpty()).count();
        assertThat(scalarCount).isGreaterThan(0);

        // --- Test VectorReadImpl: pre-filter should narrow results ---
        // Query vector near (0,1) with filter id >= 5
        // Without filter: rows 5,6,7,8,9 are closest
        // With filter id >= 5: btree pre-filter restricts to rows 5-9
        GlobalIndexResult resultWithFilter = searchBuilder.newVectorRead().read(plan);
        assertThat(resultWithFilter).isInstanceOf(ScoredGlobalIndexResult.class);
        assertThat(resultWithFilter.results().isEmpty()).isFalse();
        for (long rowId : resultWithFilter.results()) {
            assertThat(rowId).isBetween(5L, 9L);
        }

        // Compare with no-filter search (should include results from both ranges)
        GlobalIndexResult resultNoFilter =
                table.newVectorSearchBuilder()
                        .withVector(new float[] {0.1f, 0.9f})
                        .withLimit(10)
                        .withVectorColumn(VECTOR_FIELD_NAME)
                        .executeLocal();
        assertThat(resultNoFilter.results().getIntCardinality())
                .isGreaterThan(resultWithFilter.results().getIntCardinality());
    }

    private void buildAndCommitVectorIndex(FileStoreTable table, float[][] vectors, Range rowRange)
            throws Exception {
        Options options = table.coreOptions().toConfiguration();
        DataField vectorField = table.rowType().getField(VECTOR_FIELD_NAME);

        GlobalIndexSingletonWriter writer =
                (GlobalIndexSingletonWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table,
                                TestVectorGlobalIndexerFactory.IDENTIFIER,
                                vectorField,
                                options);
        for (float[] vec : vectors) {
            writer.write(vec);
        }
        List<ResultEntry> entries = writer.finish();

        List<IndexFileMeta> indexFiles =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        rowRange,
                        vectorField.id(),
                        TestVectorGlobalIndexerFactory.IDENTIFIER,
                        entries);

        DataIncrement dataIncrement = DataIncrement.indexIncrement(indexFiles);
        CommitMessage message =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        null,
                        dataIncrement,
                        CompactIncrement.emptyIncrement());
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(Collections.singletonList(message));
        }
    }

    private void buildAndCommitBTreeIndex(FileStoreTable table, int[] ids, Range rowRange)
            throws Exception {
        Options options = table.coreOptions().toConfiguration();
        DataField idField = table.rowType().getField("id");

        GlobalIndexParallelWriter writer =
                (GlobalIndexParallelWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table, BTreeGlobalIndexerFactory.IDENTIFIER, idField, options);
        for (int id : ids) {
            long relativeRowId = id - rowRange.from;
            writer.write(id, relativeRowId);
        }
        List<ResultEntry> entries = writer.finish();

        List<IndexFileMeta> indexFiles =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        rowRange,
                        idField.id(),
                        BTreeGlobalIndexerFactory.IDENTIFIER,
                        entries);

        DataIncrement dataIncrement = DataIncrement.indexIncrement(indexFiles);
        CommitMessage message =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        null,
                        dataIncrement,
                        CompactIncrement.emptyIncrement());
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(Collections.singletonList(message));
        }
    }

    private void buildAndCommitPartitionedIndex(
            FileStoreTable table, float[][] vectors, BinaryRow partition, Range rowRange)
            throws Exception {
        Options options = table.coreOptions().toConfiguration();
        DataField vectorField = table.rowType().getField(VECTOR_FIELD_NAME);

        GlobalIndexSingletonWriter writer =
                (GlobalIndexSingletonWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table,
                                TestVectorGlobalIndexerFactory.IDENTIFIER,
                                vectorField,
                                options);
        for (float[] vec : vectors) {
            writer.write(vec);
        }
        List<ResultEntry> entries = writer.finish();

        List<IndexFileMeta> indexFiles =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        rowRange,
                        vectorField.id(),
                        TestVectorGlobalIndexerFactory.IDENTIFIER,
                        entries);

        DataIncrement dataIncrement = DataIncrement.indexIncrement(indexFiles);
        CommitMessage message =
                new CommitMessageImpl(
                        partition, 0, null, dataIncrement, CompactIncrement.emptyIncrement());
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(Collections.singletonList(message));
        }
    }
}
