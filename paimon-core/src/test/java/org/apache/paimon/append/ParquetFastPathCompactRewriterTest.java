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

package org.apache.paimon.append;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.parquet.ParquetUtil;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.metrics.Counter;
import org.apache.paimon.metrics.Metric;
import org.apache.paimon.metrics.TestMetricRegistry;
import org.apache.paimon.operation.BaseAppendFileStoreWrite;
import org.apache.paimon.operation.metrics.CompactionFastPathMetrics;
import org.apache.paimon.operation.metrics.CompactionFastPathMetrics.MissReason;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.stats.SimpleStatsMerger;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.types.DataTypes;

import org.apache.paimon.shade.org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.paimon.shade.org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.paimon.shade.org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.paimon.shade.org.apache.parquet.internal.column.columnindex.OffsetIndex;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.FILE_FORMAT_PARQUET;
import static org.apache.paimon.table.BucketMode.UNAWARE_BUCKET;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ParquetFastPathCompactRewriter}. */
public class ParquetFastPathCompactRewriterTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testFastPathMatchesRewrite() throws Exception {
        PreparedTable prepared = prepareTable(Collections.emptyMap(), 5, 20);
        List<DataFileMeta> rewriteResult = compact(prepared, false);
        CompactionFastPathMetrics metrics =
                new CompactionFastPathMetrics(new TestMetricRegistry(), "test");
        List<DataFileMeta> fastPathResult = compactWithMetrics(prepared, true, metrics, null);

        assertThat(sumRows(fastPathResult)).isEqualTo(sumRows(rewriteResult));
        assertThat(fastPathResult).isNotEmpty();
        assertThat(fastPathResult.get(0).fileSource())
                .contains(org.apache.paimon.manifest.FileSource.COMPACT);
        assertThat(readRows(prepared, fastPathResult))
                .containsExactlyInAnyOrderElementsOf(readRows(prepared, rewriteResult));
        assertThat(getCounter(metrics, CompactionFastPathMetrics.HIT_COUNT)).isEqualTo(1L);
    }

    @Test
    public void testFastPathMissWhenRowTrackingEnabled() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        PreparedTable prepared = prepareTable(options, 3, 10);
        CompactionFastPathMetrics metrics =
                new CompactionFastPathMetrics(new TestMetricRegistry(), "test");

        List<DataFileMeta> rewriteResult = compact(prepared, false);
        List<DataFileMeta> fastPathResult = compactWithMetrics(prepared, true, metrics, null);

        assertThat(sumRows(fastPathResult)).isEqualTo(sumRows(rewriteResult));
        assertThat(getCounter(metrics, CompactionFastPathMetrics.HIT_COUNT)).isEqualTo(0L);
        assertThat(getCounter(metrics, missMetricName(MissReason.ROW_TRACKING))).isEqualTo(1L);
    }

    @Test
    public void testFastPathMissWhenBloomConfigured() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("parquet.bloom.filter.enabled", "true");
        PreparedTable prepared = prepareTable(options, 3, 10);
        CompactionFastPathMetrics metrics =
                new CompactionFastPathMetrics(new TestMetricRegistry(), "test");

        List<DataFileMeta> rewriteResult = compact(prepared, false);
        List<DataFileMeta> fastPathResult = compactWithMetrics(prepared, true, metrics, null);

        assertThat(sumRows(fastPathResult)).isEqualTo(sumRows(rewriteResult));
        assertThat(getCounter(metrics, CompactionFastPathMetrics.HIT_COUNT)).isEqualTo(0L);
        assertThat(getCounter(metrics, missMetricName(MissReason.BLOOM_CONFIGURED))).isEqualTo(1L);
    }

    @Test
    public void testFastPathMissWhenFileIndexConfiguredAfterWrite() throws Exception {
        PreparedTable prepared = prepareTable(Collections.emptyMap(), 3, 10);
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.APPEND_COMPACTION_ROW_GROUP_COPY_ENABLED.key(), "true");
        options.put("file-index.bloom-filter.columns", "name");
        options.put("file-index.in-manifest-threshold", "1 mb");
        FileStoreTable table = prepared.table.copy(options);
        CompactionFastPathMetrics metrics =
                new CompactionFastPathMetrics(new TestMetricRegistry(), "test");

        List<DataFileMeta> fastPathResult = tryFastPath(prepared, table, metrics, null);

        assertThat(fastPathResult).isNull();
        assertThat(getCounter(metrics, missMetricName(MissReason.FILE_INDEX))).isEqualTo(1L);

        BaseAppendFileStoreWrite write =
                (BaseAppendFileStoreWrite) table.store().newWrite(UUID.randomUUID().toString());
        List<DataFileMeta> rewriteResult =
                write.compactRewrite(prepared.partition, UNAWARE_BUCKET, null, prepared.files);
        assertThat(rewriteResult).allMatch(file -> file.embeddedIndex() != null);
    }

    @Test
    public void testFastPathAllowsDenseValueStatsCols() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("metadata.stats-dense-store", "true");
        options.put("metadata.stats-mode", "none");
        options.put("fields.name.stats-mode", "full");
        PreparedTable prepared = prepareTable(options, 3, 10);

        assertThat(prepared.files.stream().anyMatch(f -> f.valueStatsCols() != null)).isTrue();

        List<DataFileMeta> rewriteResult = compact(prepared, false);
        CompactionFastPathMetrics metrics =
                new CompactionFastPathMetrics(new TestMetricRegistry(), "test");
        List<DataFileMeta> fastPathResult = compactWithMetrics(prepared, true, metrics, null);

        assertThat(sumRows(fastPathResult)).isEqualTo(sumRows(rewriteResult));
        assertThat(readRows(prepared, fastPathResult))
                .containsExactlyInAnyOrderElementsOf(readRows(prepared, rewriteResult));
        assertThat(getCounter(metrics, CompactionFastPathMetrics.HIT_COUNT)).isEqualTo(1L);
        assertThat(fastPathResult).isNotEmpty();
        assertThat(fastPathResult.get(0).valueStats())
                .isNotEqualTo(org.apache.paimon.stats.SimpleStats.EMPTY_STATS);
    }

    @Test
    public void testFastPathWithExternalPath() throws Exception {
        java.nio.file.Path externalDir = tempDir.resolve("external-data");
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(), externalDir.toUri().toString());
        options.put(CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY.key(), "round-robin");
        PreparedTable prepared = prepareTable(options, 3, 10);

        assertThat(prepared.files.stream().allMatch(f -> f.externalPath().isPresent())).isTrue();

        List<DataFileMeta> rewriteResult = compact(prepared, false);
        List<DataFileMeta> fastPathResult = compact(prepared, true);

        assertThat(fastPathResult).isNotEmpty();
        assertThat(fastPathResult.stream().allMatch(f -> f.externalPath().isPresent())).isTrue();
        assertThat(sumRows(fastPathResult)).isEqualTo(sumRows(rewriteResult));
        assertThat(readRows(prepared, fastPathResult))
                .containsExactlyInAnyOrderElementsOf(readRows(prepared, rewriteResult));
    }

    @Test
    public void testFastPathAllowsCompactSource() throws Exception {
        PreparedTable prepared = prepareTable(Collections.emptyMap(), 4, 15);
        List<DataFileMeta> compactSources = compact(prepared, false);
        assertThat(compactSources).isNotEmpty();
        assertThat(compactSources.get(0).fileSource())
                .contains(org.apache.paimon.manifest.FileSource.COMPACT);

        PreparedTable compactPrepared =
                new PreparedTable(prepared.table, compactSources, prepared.partition);
        List<DataFileMeta> rewriteResult = compact(compactPrepared, false);
        List<DataFileMeta> fastPathResult = compact(compactPrepared, true);

        assertThat(sumRows(fastPathResult)).isEqualTo(sumRows(rewriteResult));
        assertThat(readRows(compactPrepared, fastPathResult))
                .containsExactlyInAnyOrderElementsOf(readRows(compactPrepared, rewriteResult));
    }

    @Test
    public void testFastPathWithRowGroupRolling() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.TARGET_FILE_SIZE.key(), "4 kb");
        options.put("parquet.block.size", "1024");
        PreparedTable prepared = prepareTable(options, 4, 200);

        List<DataFileMeta> rewriteResult = compact(prepared, false);
        CompactionFastPathMetrics metrics =
                new CompactionFastPathMetrics(new TestMetricRegistry(), "test");
        List<DataFileMeta> fastPathResult = compactWithMetrics(prepared, true, metrics, null);

        // forbid silent fallback to rewrite
        assertThat(getCounter(metrics, CompactionFastPathMetrics.HIT_COUNT)).isEqualTo(1L);
        assertThat(fastPathResult.size()).isGreaterThan(1);
        assertThat(sumRows(fastPathResult)).isEqualTo(sumRows(rewriteResult));
        assertThat(readRows(prepared, fastPathResult))
                .containsExactlyInAnyOrderElementsOf(readRows(prepared, rewriteResult));
        // rolling splits input RowGroups across output files, so value stats must be
        // (partially) rebuilt from block metadata; merged stats must match the rewrite path
        assertThat(mergedValueStats(prepared, fastPathResult))
                .isEqualTo(mergedValueStats(prepared, rewriteResult));
    }

    private SimpleStats mergedValueStats(PreparedTable prepared, List<DataFileMeta> files) {
        return SimpleStatsMerger.merge(
                files.stream().map(DataFileMeta::valueStats).collect(Collectors.toList()),
                prepared.table.rowType(),
                files.get(0).valueStatsCols());
    }

    @Test
    public void testFastPathWithDictionaryNullDecimalAndTimestamp() throws Exception {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("id", DataTypes.INT());
        schemaBuilder.column("price", DataTypes.DECIMAL(20, 2));
        schemaBuilder.column("ts", DataTypes.TIMESTAMP(3));
        schemaBuilder.column("tag", DataTypes.STRING());
        int rowsPerFile = 50;
        PreparedTable prepared =
                prepareTable(
                        schemaBuilder,
                        Collections.emptyMap(),
                        5,
                        rowsPerFile,
                        (file, row) -> {
                            int id = file * rowsPerFile + row;
                            return GenericRow.of(
                                    id,
                                    id % 3 == 0
                                            ? null
                                            : Decimal.fromBigDecimal(
                                                    BigDecimal.valueOf(id, 2), 20, 2),
                                    id % 4 == 0 ? null : Timestamp.fromEpochMillis(id * 1000L),
                                    // low cardinality to trigger dictionary encoding
                                    BinaryString.fromString("tag-" + (id % 5)));
                        });

        assertInputHasDictionaryEncoding(prepared);

        List<DataFileMeta> rewriteResult = compact(prepared, false);
        CompactionFastPathMetrics metrics =
                new CompactionFastPathMetrics(new TestMetricRegistry(), "test");
        List<DataFileMeta> fastPathResult = compactWithMetrics(prepared, true, metrics, null);

        // forbid silent fallback to rewrite
        assertThat(getCounter(metrics, CompactionFastPathMetrics.HIT_COUNT)).isEqualTo(1L);
        assertThat(sumRows(fastPathResult)).isEqualTo(sumRows(rewriteResult));
        List<String> fastPathRows =
                readRows(
                        prepared,
                        fastPathResult,
                        Collections.emptyList(),
                        this::complexRowToString);
        List<String> rewriteRows =
                readRows(
                        prepared, rewriteResult, Collections.emptyList(), this::complexRowToString);
        assertThat(fastPathRows).containsExactlyInAnyOrderElementsOf(rewriteRows);
        // make sure nulls are really exercised, not just an all-non-null run
        assertThat(fastPathRows.stream().filter(row -> row.contains("null")).count())
                .isGreaterThan(0);
    }

    private String complexRowToString(InternalRow row) {
        return row.getInt(0)
                + ","
                + (row.isNullAt(1) ? "null" : row.getDecimal(1, 20, 2).toString())
                + ","
                + (row.isNullAt(2) ? "null" : row.getTimestamp(2, 3).toString())
                + ","
                + row.getString(3);
    }

    private void assertInputHasDictionaryEncoding(PreparedTable prepared) throws Exception {
        DataFilePathFactory pathFactory =
                prepared.table
                        .store()
                        .pathFactory()
                        .createDataFilePathFactory(prepared.partition, UNAWARE_BUCKET);
        DataFileMeta file = prepared.files.get(0);
        Path path = pathFactory.toPath(file);
        try (ParquetFileReader reader =
                ParquetUtil.getParquetReader(
                        prepared.table.fileIO(),
                        path,
                        file.fileSize(),
                        prepared.table.coreOptions().toConfiguration())) {
            boolean hasDictionary =
                    reader.getFooter().getBlocks().stream()
                            .flatMap(block -> block.getColumns().stream())
                            .anyMatch(ColumnChunkMetaData::hasDictionaryPage);
            assertThat(hasDictionary).isTrue();
        }
    }

    @Test
    public void testFastPathPredicateReadSkipsRowGroups() throws Exception {
        // each input file is one RowGroup with a disjoint id range
        PreparedTable prepared = prepareTable(Collections.emptyMap(), 5, 100);

        CompactionFastPathMetrics metrics =
                new CompactionFastPathMetrics(new TestMetricRegistry(), "test");
        List<DataFileMeta> fastPathResult = compactWithMetrics(prepared, true, metrics, null);

        // forbid silent fallback to rewrite
        assertThat(getCounter(metrics, CompactionFastPathMetrics.HIT_COUNT)).isEqualTo(1L);

        // predicate boundary aligns with RowGroup boundaries, so stats-based RowGroup
        // pruning must skip the first three RowGroups exactly
        Predicate predicate = new PredicateBuilder(prepared.table.rowType()).greaterOrEqual(0, 300);
        List<String> filtered =
                readRows(
                        prepared,
                        fastPathResult,
                        Collections.singletonList(predicate),
                        this::rowToString);

        List<String> expected = new ArrayList<>();
        for (int id = 300; id < 500; id++) {
            expected.add(id + "," + ((long) id * 3) + ",value-" + id + "," + (id % 7));
        }
        assertThat(filtered).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testFastPathOutputFooterAlignsWithInputRowGroups() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("parquet.block.size", "1024");
        PreparedTable prepared = prepareTable(options, 4, 200);

        CompactionFastPathMetrics metrics =
                new CompactionFastPathMetrics(new TestMetricRegistry(), "test");
        List<DataFileMeta> fastPathResult = compactWithMetrics(prepared, true, metrics, null);

        // forbid silent fallback to rewrite
        assertThat(getCounter(metrics, CompactionFastPathMetrics.HIT_COUNT)).isEqualTo(1L);

        // copied output must keep the input RowGroups in order, with identical
        // row counts and per-column statistics
        List<BlockMetaData> inputBlocks = readBlocks(prepared, prepared.files);
        List<BlockMetaData> outputBlocks = readBlocks(prepared, fastPathResult);
        assertThat(outputBlocks).hasSameSizeAs(inputBlocks);
        for (int i = 0; i < inputBlocks.size(); i++) {
            BlockMetaData inputBlock = inputBlocks.get(i);
            BlockMetaData outputBlock = outputBlocks.get(i);
            assertThat(outputBlock.getRowCount()).isEqualTo(inputBlock.getRowCount());
            assertThat(outputBlock.getColumns()).hasSameSizeAs(inputBlock.getColumns());
            for (int c = 0; c < inputBlock.getColumns().size(); c++) {
                ColumnChunkMetaData inputColumn = inputBlock.getColumns().get(c);
                ColumnChunkMetaData outputColumn = outputBlock.getColumns().get(c);
                assertThat(outputColumn.getPath()).isEqualTo(inputColumn.getPath());
                assertThat(outputColumn.getTotalSize()).isEqualTo(inputColumn.getTotalSize());
                assertThat(outputColumn.getStatistics().toString())
                        .isEqualTo(inputColumn.getStatistics().toString());
            }
        }

        // column chunk offsets must stay inside the output file and blocks must be
        // laid out in increasing order starting right after the "PAR1" magic
        DataFilePathFactory pathFactory =
                prepared.table
                        .store()
                        .pathFactory()
                        .createDataFilePathFactory(prepared.partition, UNAWARE_BUCKET);
        for (DataFileMeta file : fastPathResult) {
            Path path = pathFactory.toPath(file);
            try (ParquetFileReader reader =
                    ParquetUtil.getParquetReader(
                            prepared.table.fileIO(),
                            path,
                            file.fileSize(),
                            prepared.table.coreOptions().toConfiguration())) {
                long previousBlockStart = -1;
                List<BlockMetaData> blocks = reader.getFooter().getBlocks();
                for (int i = 0; i < blocks.size(); i++) {
                    BlockMetaData block = blocks.get(i);
                    long blockStart = block.getStartingPos();
                    if (i == 0) {
                        assertThat(blockStart).isEqualTo(4L);
                    }
                    assertThat(blockStart).isGreaterThan(previousBlockStart);
                    previousBlockStart = blockStart;
                    for (ColumnChunkMetaData column : block.getColumns()) {
                        assertThat(column.getStartingPos()).isGreaterThanOrEqualTo(blockStart);
                        assertThat(column.getStartingPos() + column.getTotalSize())
                                .isLessThanOrEqualTo(file.fileSize());
                    }
                }
            }
        }
    }

    private List<BlockMetaData> readBlocks(PreparedTable prepared, List<DataFileMeta> files)
            throws Exception {
        DataFilePathFactory pathFactory =
                prepared.table
                        .store()
                        .pathFactory()
                        .createDataFilePathFactory(prepared.partition, UNAWARE_BUCKET);
        List<BlockMetaData> blocks = new ArrayList<>();
        for (DataFileMeta file : files) {
            Path path = pathFactory.toPath(file);
            try (ParquetFileReader reader =
                    ParquetUtil.getParquetReader(
                            prepared.table.fileIO(),
                            path,
                            file.fileSize(),
                            prepared.table.coreOptions().toConfiguration())) {
                blocks.addAll(reader.getFooter().getBlocks());
            }
        }
        return blocks;
    }

    @Test
    public void testFastPathOptionallyPreservesPageIndexes() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("parquet.page.size", "256");
        options.put("parquet.page.row.count.limit", "10");
        PreparedTable prepared = prepareTable(options, 3, 200);

        assertPageIndexes(prepared, prepared.files, true);

        List<DataFileMeta> withoutPageIndexes = compact(prepared, true, false);
        assertPageIndexes(prepared, withoutPageIndexes, false);

        List<DataFileMeta> withPageIndexes = compact(prepared, true, true);
        assertPageIndexes(prepared, withPageIndexes, true);
        assertThat(readRows(prepared, withPageIndexes))
                .containsExactlyInAnyOrderElementsOf(readRows(prepared, withoutPageIndexes));
    }

    @Test
    public void testFastPathMissWhenDeletionVectorPresent() throws Exception {
        PreparedTable prepared = prepareTable(Collections.emptyMap(), 3, 10);

        List<DataFileMeta> rewriteResult = compactWithDvFactory(prepared, false, fileName -> null);
        List<DataFileMeta> fastPathResult = compactWithDvFactory(prepared, true, fileName -> null);

        assertThat(sumRows(fastPathResult)).isEqualTo(sumRows(rewriteResult));
    }

    @Test
    public void testFastPathWithFooterReadParallelism() throws Exception {
        PreparedTable prepared = prepareTable(Collections.emptyMap(), 8, 20);
        List<DataFileMeta> serialResult = compactWithFooterParallelism(prepared, 1);
        List<DataFileMeta> parallelResult = compactWithFooterParallelism(prepared, 4);

        assertThat(sumRows(parallelResult)).isEqualTo(sumRows(serialResult));
        assertThat(readRows(prepared, parallelResult))
                .containsExactlyInAnyOrderElementsOf(readRows(prepared, serialResult));
    }

    @Test
    public void testFooterReadExecutorClampsParallelism() {
        ParquetFooterReadExecutor executor = new ParquetFooterReadExecutor(16);
        try {
            assertThat(executor.configuredParallelism()).isEqualTo(8);
            assertThat(executor.effectiveParallelism(3)).isEqualTo(3);
        } finally {
            executor.close();
        }
    }

    @Test
    public void testFooterReadExecutorCloseIsIdempotent() {
        ParquetFooterReadExecutor executor = new ParquetFooterReadExecutor(4);
        executor.close();
        executor.close();
    }

    @Test
    public void testFastPathMissWhenParquetWriterV2Configured() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("parquet.writer.version", "v2");
        PreparedTable prepared = prepareTable(options, 3, 10);

        List<DataFileMeta> rewriteResult = compact(prepared, false);
        List<DataFileMeta> fastPathResult = compact(prepared, true);

        assertThat(sumRows(fastPathResult)).isEqualTo(sumRows(rewriteResult));
    }

    private PreparedTable prepareTable(
            Map<String, String> extraOptions, int fileCount, int rowsPerFile) throws Exception {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("id", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.BIGINT());
        schemaBuilder.column("name", DataTypes.STRING());
        schemaBuilder.column("mod", DataTypes.INT());
        return prepareTable(
                schemaBuilder,
                extraOptions,
                fileCount,
                rowsPerFile,
                (file, row) -> {
                    int id = file * rowsPerFile + row;
                    return GenericRow.of(
                            id, (long) id * 3, BinaryString.fromString("value-" + id), id % 7);
                });
    }

    private PreparedTable prepareTable(
            Schema.Builder schemaBuilder,
            Map<String, String> extraOptions,
            int fileCount,
            int rowsPerFile,
            BiFunction<Integer, Integer, InternalRow> rowGenerator)
            throws Exception {
        FileIO fileIO = LocalFileIO.create();
        org.apache.paimon.fs.Path path =
                new org.apache.paimon.fs.Path(
                        tempDir.resolve(UUID.randomUUID().toString()).toString());
        schemaBuilder.option(CoreOptions.BUCKET.key(), "-1");
        schemaBuilder.option(CoreOptions.FILE_FORMAT.key(), CoreOptions.FILE_FORMAT_PARQUET);
        schemaBuilder.option(CoreOptions.WRITE_ONLY.key(), "true");
        schemaBuilder.option(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
        extraOptions.forEach(schemaBuilder::option);
        TableSchema tableSchema =
                new SchemaManager(fileIO, path).createTable(schemaBuilder.build());
        FileStoreTable table = FileStoreTableFactory.create(fileIO, path, tableSchema);

        String commitUser = UUID.randomUUID().toString();
        try (StreamTableWrite writer = table.newStreamWriteBuilder().newWrite()) {
            for (int file = 0; file < fileCount; file++) {
                for (int row = 0; row < rowsPerFile; row++) {
                    writer.write(rowGenerator.apply(file, row));
                }
                try (TableCommitImpl commit = table.newCommit(commitUser)) {
                    commit.commit(writer.prepareCommit(true, file));
                }
            }
        }

        List<DataFileMeta> files =
                table.store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .collect(Collectors.toList());
        return new PreparedTable(table, files, BinaryRow.EMPTY_ROW);
    }

    private List<DataFileMeta> compact(PreparedTable prepared, boolean fastPathEnabled)
            throws Exception {
        return compactWithDvFactory(prepared, fastPathEnabled, null);
    }

    private List<DataFileMeta> compact(
            PreparedTable prepared, boolean fastPathEnabled, boolean preservePageIndex)
            throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(
                CoreOptions.APPEND_COMPACTION_ROW_GROUP_COPY_ENABLED.key(),
                String.valueOf(fastPathEnabled));
        options.put(
                CoreOptions.APPEND_COMPACTION_ROW_GROUP_COPY_PRESERVE_PAGE_INDEX.key(),
                String.valueOf(preservePageIndex));
        FileStoreTable table = prepared.table.copy(options);
        BaseAppendFileStoreWrite write =
                (BaseAppendFileStoreWrite) table.store().newWrite(UUID.randomUUID().toString());
        return write.compactRewrite(prepared.partition, UNAWARE_BUCKET, null, prepared.files);
    }

    private List<DataFileMeta> compactWithMetrics(
            PreparedTable prepared,
            boolean fastPathEnabled,
            CompactionFastPathMetrics metrics,
            java.util.function.Function<String, org.apache.paimon.deletionvectors.DeletionVector>
                    dvFactory)
            throws Exception {
        if (!fastPathEnabled) {
            return compactWithDvFactory(prepared, false, dvFactory);
        }
        FileStoreTable table =
                prepared.table.copy(
                        Collections.singletonMap(
                                CoreOptions.APPEND_COMPACTION_ROW_GROUP_COPY_ENABLED.key(),
                                "true"));
        List<DataFileMeta> fastPath = tryFastPath(prepared, table, metrics, dvFactory);
        if (fastPath != null) {
            return fastPath;
        }
        return compactWithDvFactory(prepared, false, dvFactory);
    }

    private List<DataFileMeta> tryFastPath(
            PreparedTable prepared,
            FileStoreTable table,
            CompactionFastPathMetrics metrics,
            java.util.function.Function<String, org.apache.paimon.deletionvectors.DeletionVector>
                    dvFactory)
            throws Exception {
        DataFilePathFactory pathFactory =
                table.store()
                        .pathFactory()
                        .createDataFilePathFactory(prepared.partition, UNAWARE_BUCKET);
        List<DataFileMeta> fastPath =
                ParquetFastPathCompactRewriter.tryRewrite(
                        table.fileIO(),
                        org.apache.paimon.format.FileFormat.fromIdentifier(
                                FILE_FORMAT_PARQUET, table.coreOptions().toConfiguration()),
                        table.rowType(),
                        table.coreOptions(),
                        prepared.partition,
                        UNAWARE_BUCKET,
                        dvFactory,
                        prepared.files,
                        pathFactory,
                        prepared.files.get(0).schemaId(),
                        metrics,
                        new ParquetFooterReadExecutor(
                                table.coreOptions()
                                        .appendCompactionRowGroupCopyFooterReadParallelism()));
        return fastPath;
    }

    private List<DataFileMeta> compactWithFooterParallelism(
            PreparedTable prepared, int footerReadParallelism) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.APPEND_COMPACTION_ROW_GROUP_COPY_ENABLED.key(), "true");
        options.put(
                CoreOptions.APPEND_COMPACTION_ROW_GROUP_COPY_FOOTER_READ_PARALLELISM.key(),
                String.valueOf(footerReadParallelism));
        FileStoreTable table = prepared.table.copy(options);
        BaseAppendFileStoreWrite write =
                (BaseAppendFileStoreWrite) table.store().newWrite(UUID.randomUUID().toString());
        try {
            return write.compactRewrite(prepared.partition, UNAWARE_BUCKET, null, prepared.files);
        } finally {
            write.close();
        }
    }

    private long getCounter(CompactionFastPathMetrics metrics, String metricName) {
        Metric metric = metrics.getMetricGroup().getMetrics().get(metricName);
        return ((Counter) metric).getCount();
    }

    private static String missMetricName(MissReason reason) {
        return CompactionFastPathMetrics.MISS_COUNT_PREFIX
                + Character.toUpperCase(reason.name().charAt(0))
                + reason.name().substring(1).toLowerCase();
    }

    private List<DataFileMeta> compactWithDvFactory(
            PreparedTable prepared,
            boolean fastPathEnabled,
            java.util.function.Function<String, org.apache.paimon.deletionvectors.DeletionVector>
                    dvFactory)
            throws Exception {
        FileStoreTable table =
                prepared.table.copy(
                        Collections.singletonMap(
                                CoreOptions.APPEND_COMPACTION_ROW_GROUP_COPY_ENABLED.key(),
                                String.valueOf(fastPathEnabled)));
        BaseAppendFileStoreWrite write =
                (BaseAppendFileStoreWrite) table.store().newWrite(UUID.randomUUID().toString());
        return write.compactRewrite(prepared.partition, UNAWARE_BUCKET, dvFactory, prepared.files);
    }

    private List<String> readRows(PreparedTable prepared, List<DataFileMeta> files)
            throws Exception {
        return readRows(prepared, files, Collections.emptyList(), this::rowToString);
    }

    private List<String> readRows(
            PreparedTable prepared,
            List<DataFileMeta> files,
            List<Predicate> filters,
            Function<InternalRow, String> rowConverter)
            throws Exception {
        FileStoreTable table = prepared.table;
        FormatReaderFactory readerFactory =
                FileFormat.fromIdentifier(
                                FILE_FORMAT_PARQUET, table.coreOptions().toConfiguration())
                        .createReaderFactory(table.rowType(), table.rowType(), filters);
        DataFilePathFactory pathFactory =
                table.store()
                        .pathFactory()
                        .createDataFilePathFactory(prepared.partition, UNAWARE_BUCKET);
        FileIO fileIO = table.fileIO();
        List<String> rows = new ArrayList<>();
        for (DataFileMeta file : files) {
            Path path = pathFactory.toPath(file);
            try (FileRecordReader<InternalRow> reader =
                    readerFactory.createReader(
                            new FormatReaderContext(fileIO, path, file.fileSize()))) {
                RecordReader.RecordIterator<InternalRow> iterator = reader.readBatch();
                while (iterator != null) {
                    InternalRow row;
                    while ((row = iterator.next()) != null) {
                        rows.add(rowConverter.apply(row));
                    }
                    iterator.releaseBatch();
                    iterator = reader.readBatch();
                }
            }
        }
        return rows;
    }

    private String rowToString(InternalRow row) {
        return row.getInt(0)
                + ","
                + row.getLong(1)
                + ","
                + row.getString(2).toString()
                + ","
                + row.getInt(3);
    }

    private void assertPageIndexes(
            PreparedTable prepared, List<DataFileMeta> files, boolean expected) throws Exception {
        DataFilePathFactory pathFactory =
                prepared.table
                        .store()
                        .pathFactory()
                        .createDataFilePathFactory(prepared.partition, UNAWARE_BUCKET);
        for (DataFileMeta file : files) {
            Path path = pathFactory.toPath(file);
            try (ParquetFileReader reader =
                    ParquetUtil.getParquetReader(
                            prepared.table.fileIO(),
                            path,
                            file.fileSize(),
                            prepared.table.coreOptions().toConfiguration())) {
                for (BlockMetaData block : reader.getFooter().getBlocks()) {
                    for (ColumnChunkMetaData column : block.getColumns()) {
                        assertThat(column.getColumnIndexReference() != null).isEqualTo(expected);
                        assertThat(column.getOffsetIndexReference() != null).isEqualTo(expected);
                        if (expected) {
                            assertThat(reader.readColumnIndex(column)).isNotNull();
                            OffsetIndex offsetIndex = reader.readOffsetIndex(column);
                            assertThat(offsetIndex).isNotNull();
                            assertThat(offsetIndex.getPageCount()).isGreaterThan(0);
                            assertThat(offsetIndex.getOffset(0))
                                    .isBetween(
                                            column.getStartingPos(),
                                            column.getStartingPos() + column.getTotalSize() - 1);
                        }
                    }
                }
            }
        }
    }

    private long sumRows(List<DataFileMeta> files) {
        return files.stream().mapToLong(DataFileMeta::rowCount).sum();
    }

    private static final class PreparedTable {
        private final FileStoreTable table;
        private final List<DataFileMeta> files;
        private final BinaryRow partition;

        private PreparedTable(FileStoreTable table, List<DataFileMeta> files, BinaryRow partition) {
            this.table = table;
            this.files = files;
            this.partition = partition;
        }
    }
}
