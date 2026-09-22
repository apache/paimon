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
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.CastTransform;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FieldTransform;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.Transform;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilderImpl;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.InnerTableCommit;
import org.apache.paimon.table.sink.InnerTableWrite;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.source.ChainSplit;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.QueryAuthSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.BUCKET_KEY;
import static org.apache.paimon.CoreOptions.CHAIN_TABLE_CHAIN_PARTITION_KEYS;
import static org.apache.paimon.CoreOptions.CHAIN_TABLE_ENABLED;
import static org.apache.paimon.CoreOptions.CHANGELOG_PRODUCER;
import static org.apache.paimon.CoreOptions.DELETION_VECTORS_ENABLED;
import static org.apache.paimon.CoreOptions.MERGE_ENGINE;
import static org.apache.paimon.CoreOptions.PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE;
import static org.apache.paimon.CoreOptions.PARTITION_TIMESTAMP_FORMATTER;
import static org.apache.paimon.CoreOptions.PARTITION_TIMESTAMP_PATTERN;
import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.CoreOptions.SEQUENCE_FIELD;
import static org.apache.paimon.catalog.Identifier.DEFAULT_MAIN_BRANCH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ChainTableFileStoreTable}. */
public class ChainTableFileStoreTableTest {
    @TempDir java.nio.file.Path tempDir;

    private static final String SNAPSHOT_BRANCH = "snapshot";
    private static final String DELTA_BRANCH = "delta";

    private String commitUser;
    private String tableName;

    @BeforeEach
    public void beforeEach() {
        String uuid = UUID.randomUUID().toString();
        commitUser = uuid;
        tableName = "chain_t_" + uuid.replace("-", "");
    }

    private static Stream<Arguments> filterPushdownParams() {
        return Stream.of("parquet", "orc")
                .flatMap(
                        format ->
                                Stream.of(false, true)
                                        .flatMap(
                                                dv ->
                                                        Stream.of(false, true)
                                                                .map(
                                                                        keyRange ->
                                                                                Arguments.of(
                                                                                        format, dv,
                                                                                        keyRange))));
    }

    @ParameterizedTest
    @MethodSource("filterPushdownParams")
    public void testFilterPushdownAcrossChainMerge(
            String format, boolean deletionVectors, boolean keyRangeSplit) throws Exception {
        createChainTable(
                options -> {
                    options.set(CoreOptions.FILE_FORMAT, format);
                    options.set(DELETION_VECTORS_ENABLED, deletionVectors);
                    options.set(CoreOptions.CHAIN_TABLE_KEY_RANGE_SPLIT_ENABLED, keyRangeSplit);
                    options.set(CoreOptions.SOURCE_SPLIT_TARGET_SIZE, new MemorySize(1));
                });
        FileStoreTable table = loadTable();
        FileStoreTable snapshot = table.switchToBranch(SNAPSHOT_BRANCH);
        FileStoreTable delta = table.switchToBranch(DELTA_BRANCH);
        // Separate files let branch-local stats discard individual versions independently.
        writeWithCommit(snapshot, row(1L, 1L, "old", "CN", "20250810", "20"));
        writeWithCommit(snapshot, row(2L, 1L, "new", "CN", "20250810", "20"));
        if (!deletionVectors) {
            writeWithCommit(snapshot, row(3L, 1L, "old", "CN", "20250810", "20"));
        }
        writeWithCommit(snapshot, row(4L, 1L, "old", "CN", "20250810", "20"));
        writeWithCommit(snapshot, row(5L, 1L, null, "CN", "20250810", "20"));
        writeWithCommit(snapshot, row(6L, 1L, "old", "CN", "20250810", "20"));
        writeWithCommit(snapshot, row(1L, 1L, "other-group", "US", "20250810", "20"));
        writeWithCommit(delta, row(1L, 2L, "new", "CN", "20250810", "21"));
        writeWithCommit(delta, row(2L, 2L, "old", "CN", "20250810", "21"));
        if (!deletionVectors) {
            writeWithCommit(delta, row(RowKind.DELETE, 3L, 2L, "new", "CN", "20250810", "21"));
        }
        writeWithCommit(delta, row(4L, 2L, null, "CN", "20250810", "21"));
        writeWithCommit(delta, row(5L, 2L, "old", "CN", "20250810", "21"));
        writeWithCommit(delta, row(6L, 2L, "new", "CN", "20250810", "21"));
        writeWithCommit(delta, row(7L, 1L, "old", "CN", "20250810", "21"));
        writeWithCommit(delta, row(6L, 3L, "latest", "CN", "20250810", "22"));
        writeWithCommit(delta, row(1L, 2L, "other-group-new", "US", "20250810", "22"));
        table = loadTable();
        Map<String, String> partition =
                ImmutableMap.of("region", "CN", "dt", "20250810", "hour", "22");
        List<GenericRow> baseline = getResult(table, partition);
        assertThat(baseline)
                .containsExactlyInAnyOrder(
                        row(1L, 2L, "new", "CN", "20250810", "22"),
                        row(2L, 2L, "old", "CN", "20250810", "22"),
                        row(4L, 2L, null, "CN", "20250810", "22"),
                        row(5L, 2L, "old", "CN", "20250810", "22"),
                        row(6L, 3L, "latest", "CN", "20250810", "22"),
                        row(7L, 1L, "old", "CN", "20250810", "22"));
        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        Predicate old = builder.equal(2, BinaryString.fromString("old"));
        Predicate newer = builder.equal(2, BinaryString.fromString("new"));
        for (Predicate filter :
                Arrays.asList(
                        old,
                        newer,
                        builder.isNull(2),
                        builder.isNotNull(2),
                        old.negate().get(),
                        builder.greaterThan(1, 1L),
                        builder.equal(0, 1L),
                        builder.equal(
                                new CastTransform(
                                        new FieldRef(0, "k", DataTypes.BIGINT()),
                                        DataTypes.STRING()),
                                BinaryString.fromString("1")),
                        PredicateBuilder.and(builder.equal(0, 1L), old),
                        PredicateBuilder.or(builder.equal(0, 1L), old),
                        PredicateBuilder.or(builder.equal(0, 1L), builder.equal(0, 6L)),
                        PredicateBuilder.or(
                                PredicateBuilder.and(builder.equal(0, 1L), old),
                                PredicateBuilder.and(builder.equal(0, 6L), newer)),
                        builder.equal(2, BinaryString.fromString("absent")),
                        PredicateBuilder.alwaysTrue(),
                        PredicateBuilder.alwaysFalse())) {
            assertFilterMatchesMergedRows(table, partition, filter, baseline);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"parquet", "orc"})
    public void testPartialUpdateFilterPushdownAcrossChainMerge(String format) throws Exception {
        createPartialUpdateChainTable(
                options -> {
                    options.set(CoreOptions.FILE_FORMAT, format);
                    options.set(PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE, true);
                });
        FileStoreTable table = loadTable();
        FileStoreTable snapshot = table.switchToBranch(SNAPSHOT_BRANCH);
        FileStoreTable delta = table.switchToBranch(DELTA_BRANCH);
        writeWithCommit(snapshot, row(1L, 1L, "a", null, "20250810"));
        writeWithCommit(snapshot, row(2L, 1L, "a", "old", "20250810"));
        writeWithCommit(delta, row(1L, 2L, null, "b", "20250811"));
        writeWithCommit(delta, row(RowKind.DELETE, 2L, 2L, null, null, "20250811"));
        table = loadTable();
        Map<String, String> partition = ImmutableMap.of("dt", "20250811");
        List<GenericRow> baseline = getResult(table, partition);
        assertThat(baseline).containsExactly(row(1L, 2L, "a", "b", "20250811"));
        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        Predicate a = builder.equal(2, BinaryString.fromString("a"));
        Predicate b = builder.equal(3, BinaryString.fromString("b"));
        for (Predicate filter :
                Arrays.asList(
                        a,
                        b,
                        PredicateBuilder.and(a, b),
                        PredicateBuilder.or(a, b),
                        builder.isNull(3),
                        builder.equal(0, 2L),
                        PredicateBuilder.and(builder.equal(0, 1L), a))) {
            assertFilterMatchesMergedRows(table, partition, filter, baseline);
        }
    }

    @Test
    public void testChainFilterPreservesKeyAndCompleteSnapshotPruning() throws Exception {
        createChainTable(options -> options.set(BUCKET, 2));
        FileStoreTable table = loadTable();
        FileStoreTable snapshot = table.switchToBranch(SNAPSHOT_BRANCH);
        FileStoreTable delta = table.switchToBranch(DELTA_BRANCH);
        for (long key = 1; key <= 4; key++) {
            writeWithCommit(snapshot, row(key, 1L, "old", "CN", "20250810", "20"));
            writeWithCommit(delta, row(key, 2L, "new", "CN", "20250810", "21"));
        }
        table = loadTable();
        Map<String, String> partition =
                ImmutableMap.of("region", "CN", "dt", "20250810", "hour", "21");
        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        Predicate key = builder.equal(0, 1L);
        Predicate old = builder.equal(2, BinaryString.fromString("old"));
        long allBytes = plannedFileBytes(table, partition, PredicateBuilder.alwaysTrue());
        long keyBytes = plannedFileBytes(table, partition, key);
        assertThat(keyBytes).isPositive().isLessThan(allBytes);
        assertThat(plannedFileBytes(table, partition, PredicateBuilder.and(key, old)))
                .isEqualTo(keyBytes);
        Predicate keyOr = PredicateBuilder.or(key, builder.equal(0, 2L));
        Predicate mixedOr =
                PredicateBuilder.or(
                        PredicateBuilder.and(key, old),
                        PredicateBuilder.and(builder.equal(0, 2L), builder.isNull(2)));
        assertThat(plannedFileBytes(table, partition, mixedOr))
                .isEqualTo(plannedFileBytes(table, partition, keyOr));
        assertThat(plannedFileBytes(table, partition, mixedOr)).isLessThan(allBytes);
        assertThat(plannedFileBytes(table, partition, PredicateBuilder.or(key, old)))
                .isEqualTo(allBytes);
        Map<String, String> complete =
                ImmutableMap.of("region", "CN", "dt", "20250810", "hour", "20");
        assertThat(plannedFileBytes(table, complete, old)).isPositive();
        assertThat(
                        plannedFileBytes(
                                table,
                                complete,
                                builder.equal(2, BinaryString.fromString("absent"))))
                .isZero();
        assertThat(
                        table.newReadBuilder()
                                .withPartitionFilter(partition)
                                .withFilter(PredicateBuilder.and(key, old))
                                .withBucketFilter(bucket -> false)
                                .newScan()
                                .plan()
                                .splits())
                .isEmpty();
        long bucketBytes = 0;
        for (int bucket = 0; bucket < 2; bucket++) {
            final int selectedBucket = bucket;
            TableScan.Plan plan =
                    table.newReadBuilder()
                            .withPartitionFilter(partition)
                            .withFilter(PredicateBuilder.alwaysTrue())
                            .withBucketFilter(candidate -> candidate == selectedBucket)
                            .newScan()
                            .plan();
            long bytes =
                    plan.splits().stream()
                            .flatMap(this::plannedDataFiles)
                            .mapToLong(file -> file.fileSize())
                            .sum();
            assertThat(bytes).isPositive().isLessThan(allBytes);
            bucketBytes += bytes;
        }
        assertThat(bucketBytes).isEqualTo(allBytes);
    }

    @Test
    public void testChainKeyFilterAfterSchemaReorder() throws Exception {
        createChainTable(options -> {});
        writeWithCommit(
                loadTable().switchToBranch(SNAPSHOT_BRANCH),
                row(9L, 1L, "old", "CN", "20250810", "20"));
        Path path = new Path(tempDir.toUri().toString(), tableName);
        for (String branch : Arrays.asList(DEFAULT_MAIN_BRANCH, SNAPSHOT_BRANCH, DELTA_BRANCH)) {
            new FileSystemSchemaManager(LocalFileIO.create(), path, branch)
                    .commitChanges(SchemaChange.updateColumnPosition(SchemaChange.Move.last("k")));
        }
        FileStoreTable table = loadTable();
        writeWithCommit(
                table.switchToBranch(DELTA_BRANCH), row(2L, "new", "CN", "20250810", "21", 9L));
        table = loadTable();
        assertThat(table.rowType().getFieldIndex("k")).isEqualTo(5);
        Map<String, String> partition =
                ImmutableMap.of("region", "CN", "dt", "20250810", "hour", "21");
        List<GenericRow> baseline = getResult(table, partition);
        assertThat(baseline).containsExactly(row(2L, "new", "CN", "20250810", "21", 9L));
        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        assertFilterMatchesMergedRows(table, partition, builder.equal(5, 9L), baseline);
        assertFilterMatchesMergedRows(
                table,
                partition,
                PredicateBuilder.and(
                        builder.equal(5, 9L), builder.equal(1, BinaryString.fromString("old"))),
                baseline);
    }

    @ParameterizedTest
    @ValueSource(strings = {"parquet", "orc"})
    public void testFilterPushdownWithoutSnapshotAnchor(String format) throws Exception {
        createChainTable(options -> options.set(CoreOptions.FILE_FORMAT, format));
        FileStoreTable delta = loadTable().switchToBranch(DELTA_BRANCH);
        writeWithCommit(delta, row(1L, 1L, "old", "CN", "20250810", "20"));
        writeWithCommit(delta, row(2L, 1L, "old", "CN", "20250810", "20"));
        writeWithCommit(delta, row(1L, 2L, "new", "CN", "20250810", "21"));
        writeWithCommit(delta, row(RowKind.DELETE, 2L, 2L, "new", "CN", "20250810", "21"));
        FileStoreTable table = loadTable();
        Map<String, String> partition =
                ImmutableMap.of("region", "CN", "dt", "20250810", "hour", "21");
        List<GenericRow> baseline = getResult(table, partition);
        assertThat(baseline).containsExactly(row(1L, 2L, "new", "CN", "20250810", "21"));
        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        for (Predicate filter :
                Arrays.asList(
                        builder.equal(2, BinaryString.fromString("old")),
                        builder.equal(2, BinaryString.fromString("new")),
                        builder.equal(0, 2L),
                        PredicateBuilder.and(builder.equal(0, 1L), builder.greaterThan(1, 1L)))) {
            assertFilterMatchesMergedRows(table, partition, filter, baseline);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"parquet", "orc"})
    public void testFilterPushdownAgainstVersionHistory(String format) throws Exception {
        createChainTable(options -> options.set(CoreOptions.FILE_FORMAT, format));
        FileStoreTable table = loadTable();
        Map<Long, GenericRow> latest = new HashMap<>();
        for (long key = 0; key < 24; key++) {
            String value = key % 4 == 0 ? null : "v" + key % 3;
            writeWithCommit(
                    table.switchToBranch(SNAPSHOT_BRANCH),
                    row(key, 0L, value, "CN", "20250810", "20"));
            latest.put(key, row(key, 0L, value, "CN", "20250810", "23"));
        }
        Random random = new Random(20260919L);
        FileStoreTable delta = table.switchToBranch(DELTA_BRANCH);
        for (int version = 1; version <= 3; version++) {
            List<GenericRow> updates = new ArrayList<>();
            for (long key = 0; key < 28; key++) {
                if (random.nextInt(3) == 0) {
                    continue;
                }
                boolean delete = random.nextInt(5) == 0;
                String value = random.nextInt(4) == 0 ? null : "v" + random.nextInt(3);
                updates.add(
                        row(
                                delete ? RowKind.DELETE : RowKind.INSERT,
                                key,
                                (long) version,
                                value,
                                "CN",
                                "20250810",
                                Integer.toString(20 + version)));
                if (delete) {
                    latest.remove(key);
                } else {
                    latest.put(key, row(key, (long) version, value, "CN", "20250810", "23"));
                }
            }
            writeWithCommit(delta, updates.toArray(new GenericRow[0]));
        }
        table = loadTable();
        Map<String, String> partition =
                ImmutableMap.of("region", "CN", "dt", "20250810", "hour", "23");
        List<GenericRow> expectedRows = new ArrayList<>(latest.values());
        assertThat(getResult(table, partition)).containsExactlyInAnyOrderElementsOf(expectedRows);
        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        for (int i = 0; i < 24; i++) {
            Predicate key = builder.greaterOrEqual(0, (long) random.nextInt(28));
            Predicate value = builder.equal(2, BinaryString.fromString("v" + random.nextInt(3)));
            Predicate predicate;
            switch (i % 4) {
                case 0:
                    predicate = PredicateBuilder.and(key, value);
                    break;
                case 1:
                    predicate = PredicateBuilder.or(key, value);
                    break;
                case 2:
                    predicate =
                            PredicateBuilder.or(
                                    PredicateBuilder.and(key, builder.isNull(2)),
                                    PredicateBuilder.and(builder.lessThan(0, 12L), value));
                    break;
                default:
                    predicate = PredicateBuilder.and(key, value.negate().get());
            }
            assertFilterMatchesMergedRows(table, partition, predicate, expectedRows);
        }
    }

    @Test
    public void testFilterCombiningLogicalPartitionsAndValues() throws Exception {
        createChainTable(options -> {});
        FileStoreTable table = loadTable();
        FileStoreTable snapshot = table.switchToBranch(SNAPSHOT_BRANCH);
        FileStoreTable delta = table.switchToBranch(DELTA_BRANCH);
        writeWithCommit(snapshot, row(1L, 1L, "old", "CN", "20250810", "20"));
        writeWithCommit(snapshot, row(2L, 1L, "old", "CN", "20250810", "20"));
        writeWithCommit(delta, row(1L, 2L, "new", "CN", "20250810", "21"));
        writeWithCommit(delta, row(1L, 3L, "latest", "CN", "20250810", "22"));
        table = loadTable();
        List<GenericRow> baseline = getResult(table, null);
        assertThat(baseline)
                .containsExactlyInAnyOrder(
                        row(1L, 1L, "old", "CN", "20250810", "20"),
                        row(2L, 1L, "old", "CN", "20250810", "20"),
                        row(1L, 2L, "new", "CN", "20250810", "21"),
                        row(2L, 1L, "old", "CN", "20250810", "21"),
                        row(1L, 3L, "latest", "CN", "20250810", "22"),
                        row(2L, 1L, "old", "CN", "20250810", "22"));
        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        Predicate filter =
                PredicateBuilder.or(
                        PredicateBuilder.and(
                                builder.equal(5, BinaryString.fromString("22")),
                                builder.equal(0, 1L)),
                        PredicateBuilder.and(
                                builder.equal(5, BinaryString.fromString("21")),
                                builder.equal(2, BinaryString.fromString("old"))));
        assertFilterMatchesMergedRows(table, null, filter, baseline);
    }

    @Test
    public void testKeyFilterRespectsQueryAuthMask() throws Exception {
        createChainTableWithQueryAuth();
        Transform mask = new FieldTransform(new FieldRef(1, "seq", DataTypes.BIGINT()));
        FileStoreTable table =
                loadTable(
                        queryAuthEnvironment(
                                () ->
                                        new TableQueryAuthResult(
                                                null,
                                                Collections.singletonMap(
                                                        "k", JsonSerdeUtil.toFlatJson(mask)))));
        List<GenericRow> baseline = getResult(table, QUERIED_PARTITION);
        assertThat(baseline)
                .containsExactlyInAnyOrder(
                        row(2L, 2L, "1-1", "CN", "20250810", "21"),
                        row(1L, 1L, "2", "CN", "20250810", "21"),
                        row(1L, 1L, "4", "CN", "20250810", "21"));
        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        assertFilterMatchesMergedRows(table, QUERIED_PARTITION, builder.equal(0, 1L), baseline);
    }

    private void assertFilterMatchesMergedRows(
            FileStoreTable table,
            Map<String, String> partition,
            Predicate filter,
            List<GenericRow> mergedRows)
            throws Exception {
        List<GenericRow> expected =
                mergedRows.stream().filter(filter::test).collect(Collectors.toList());
        ReadBuilder readBuilder =
                table.newReadBuilder().withPartitionFilter(partition).withFilter(filter);
        List<GenericRow> actual = new ArrayList<>();
        InternalRowSerializer serializer = new InternalRowSerializer(table.rowType());
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            reader.forEachRemaining(
                    record -> {
                        // Keep the residual: extra rows are allowed by pushdown, missing versions
                        // are not.
                        if (filter.test(record)) {
                            actual.add((GenericRow) serializer.copy(record));
                        }
                    });
        }
        assertThat(actual)
                .as("Pushed predicate %s", filter)
                .containsExactlyInAnyOrderElementsOf(expected);
    }

    private Stream<DataFileMeta> plannedDataFiles(Split split) {
        if (split instanceof FallbackReadFileStoreTable.FallbackSplit) {
            return plannedDataFiles(((FallbackReadFileStoreTable.FallbackSplit) split).wrapped());
        }
        return split instanceof ChainSplit
                ? ((ChainSplit) split).dataFiles().stream()
                : ((DataSplit) split).dataFiles().stream();
    }

    private long plannedFileBytes(
            FileStoreTable table, Map<String, String> partition, Predicate filter) {
        return table.newReadBuilder().withPartitionFilter(partition).withFilter(filter).newScan()
                .plan().splits().stream()
                .flatMap(this::plannedDataFiles)
                .mapToLong(file -> file.fileSize())
                .sum();
    }

    @Test
    public void testChainTableRejectsAggregate() throws Exception {
        assertThatThrownBy(
                        () ->
                                createChainTable(
                                        options -> {
                                            options.set(
                                                    MERGE_ENGINE,
                                                    CoreOptions.MergeEngine.AGGREGATE);
                                            options.set("fields.seq.aggregate-function", "max");
                                            options.set("fields.v.aggregate-function", "min");
                                        }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Should not define aggregation on sequence field: 'seq'.");
    }

    @Test
    public void testChainTableRejectsFirstRow() throws Exception {
        assertThatThrownBy(
                        () ->
                                createChainTable(
                                        options -> {
                                            options.set(
                                                    MERGE_ENGINE,
                                                    CoreOptions.MergeEngine.FIRST_ROW);
                                        }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Do not support use sequence field on FIRST_ROW merge engine.");
    }

    @Test
    public void testChainTableWithPartialUpdateDelete() throws Exception {
        createPartialUpdateChainTable(
                options -> {
                    options.set(PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE, true);
                });
        FileStoreTable chainTable = loadTable();
        FileStoreTable snapshotTable = chainTable.switchToBranch(SNAPSHOT_BRANCH);
        FileStoreTable deltaTable = chainTable.switchToBranch(DELTA_BRANCH);
        writeWithCommit(
                snapshotTable,
                row(1L, 1L, "a", null, "20250810"),
                row(2L, 1L, null, "B", "20250810"));

        writeWithCommit(
                deltaTable,
                row(1L, 2L, null, "A1", "20250811"),
                row(2L, 2L, "b1", null, "20250811"),
                row(5L, 1L, "e", "E", "20250811"));

        writeWithCommit(
                deltaTable,
                row(RowKind.DELETE, 1L, 3L, null, null, "20250811"),
                row(RowKind.UPDATE_BEFORE, 2L, 3L, "b1", "B", "20250811"),
                row(RowKind.UPDATE_AFTER, 2L, 4L, "b2", "B", "20250812"),
                row(RowKind.DELETE, 5L, 2L, null, null, "20250812"));

        assertThat(getResult(loadTable(), ImmutableMap.of("dt", "20250811")))
                .containsExactlyInAnyOrder(
                        row(2L, 2L, "b1", "B", "20250811"), row(5L, 1L, "e", "E", "20250811"));

        assertThat(getResult(loadTable(), ImmutableMap.of("dt", "20250812")))
                .containsExactlyInAnyOrder(row(2L, 4L, "b2", "B", "20250812"));
    }

    @Test
    public void testChainTableRejectsPartialUpdateWithDeletionVectors() throws Exception {
        assertThatThrownBy(
                        () ->
                                createPartialUpdateChainTable(
                                        options -> {
                                            options.set(DELETION_VECTORS_ENABLED, true);
                                            options.set(
                                                    PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE, true);
                                        }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Chain tables only support deletion vectors with the deduplicate merge engine.");
    }

    private static Stream<Arguments> dvChangelogParams() {
        return Stream.of(
                Arguments.of(false, CoreOptions.ChangelogProducer.NONE),
                Arguments.of(true, CoreOptions.ChangelogProducer.NONE),
                Arguments.of(false, CoreOptions.ChangelogProducer.INPUT),
                Arguments.of(true, CoreOptions.ChangelogProducer.INPUT));
    }

    @ParameterizedTest
    @MethodSource("dvChangelogParams")
    public void testChainTableWithDelete(
            boolean deletionVectors, CoreOptions.ChangelogProducer changelog) throws Exception {
        createChainTable(
                options -> {
                    options.set(DELETION_VECTORS_ENABLED, deletionVectors);
                    options.set(CHANGELOG_PRODUCER, changelog);
                });
        FileStoreTable chainTable = loadTable();
        FileStoreTable snapshotTable = chainTable.switchToBranch(SNAPSHOT_BRANCH);
        writeWithCommit(
                snapshotTable,
                row(1L, 1L, "1", "CN", "20250810", "20"),
                row(2L, 1L, "2", "CN", "20250810", "20"),
                row(3L, 1L, "3", "US", "20250810", "20"));
        writeWithCommit(snapshotTable, row(RowKind.DELETE, 2L, 1L, "2", "CN", "20250810", "20"));

        assertThat(getResult(snapshotTable, ImmutableMap.of("dt", "20250810", "hour", "20")))
                .containsExactlyInAnyOrder(
                        row(1L, 1L, "1", "CN", "20250810", "20"),
                        row(3L, 1L, "3", "US", "20250810", "20"));

        FileStoreTable deltaTable = chainTable.switchToBranch(DELTA_BRANCH);
        writeWithCommit(
                deltaTable,
                row(1L, 2L, "1-1", "CN", "20250810", "21"),
                row(4L, 1L, "4", "CN", "20250810", "21"),
                row(5L, 1L, "5", "US", "20250810", "21"),
                row(6L, 1L, "6", "UK", "20250810", "21"));
        writeWithCommit(deltaTable, row(RowKind.DELETE, 4L, 2L, "4", "CN", "20250810", "21"));

        assertThat(getResult(deltaTable, ImmutableMap.of("dt", "20250810", "hour", "21")))
                .containsExactlyInAnyOrder(
                        row(1L, 2L, "1-1", "CN", "20250810", "21"),
                        row(5L, 1L, "5", "US", "20250810", "21"),
                        row(6L, 1L, "6", "UK", "20250810", "21"));

        assertThat(getResult(loadTable(), ImmutableMap.of("dt", "20250810", "hour", "21")))
                .containsExactlyInAnyOrder(
                        row(1L, 2L, "1-1", "CN", "20250810", "21"),
                        row(3L, 1L, "3", "US", "20250810", "21"),
                        row(5L, 1L, "5", "US", "20250810", "21"),
                        row(6L, 1L, "6", "UK", "20250810", "21"));

        assertChainSplitCarriesDeletionFiles(
                deletionVectors, ImmutableMap.of("region", "CN", "dt", "20250810", "hour", "21"));

        // test cross partition delete
        writeWithCommit(deltaTable, row(RowKind.DELETE, 5L, 2L, "5", "US", "20250810", "22"));
        assertThat(getResult(loadTable(), ImmutableMap.of("dt", "20250810", "hour", "22")))
                .containsExactlyInAnyOrder(row(3L, 1L, "3", "US", "20250810", "22"));
    }

    @ParameterizedTest
    @MethodSource("dvChangelogParams")
    public void testChainTableWithUpdate(
            boolean deletionVectors, CoreOptions.ChangelogProducer changelog) throws Exception {
        createChainTable(
                options -> {
                    options.set(DELETION_VECTORS_ENABLED, deletionVectors);
                    options.set(CHANGELOG_PRODUCER, changelog);
                });
        FileStoreTable chainTable = loadTable();
        FileStoreTable snapshotTable = chainTable.switchToBranch(SNAPSHOT_BRANCH);
        writeWithCommit(
                snapshotTable,
                row(1L, 1L, "1", "CN", "20250810", "20"),
                row(2L, 1L, "2", "CN", "20250810", "20"));
        writeWithCommit(
                snapshotTable,
                row(RowKind.UPDATE_BEFORE, 2L, 1L, "2", "CN", "20250810", "20"),
                row(RowKind.UPDATE_AFTER, 2L, 2L, "2-1", "CN", "20250810", "20"));

        FileStoreTable deltaTable = chainTable.switchToBranch(DELTA_BRANCH);
        writeWithCommit(
                deltaTable,
                row(1L, 2L, "1-1", "CN", "20250811", "20"),
                row(3L, 1L, "3", "CN", "20250811", "20"));
        writeWithCommit(
                deltaTable,
                row(RowKind.UPDATE_BEFORE, 3L, 1L, "3", "CN", "20250811", "20"),
                row(RowKind.UPDATE_AFTER, 3L, 2L, "3-1", "CN", "20250811", "20"));

        assertThat(getResult(loadTable(), ImmutableMap.of("dt", "20250811", "hour", "20")))
                .containsExactlyInAnyOrder(
                        row(RowKind.INSERT, 1L, 2L, "1-1", "CN", "20250811", "20"),
                        row(RowKind.UPDATE_AFTER, 2L, 2L, "2-1", "CN", "20250811", "20"),
                        row(RowKind.UPDATE_AFTER, 3L, 2L, "3-1", "CN", "20250811", "20"));

        assertChainSplitCarriesDeletionFiles(
                deletionVectors, ImmutableMap.of("region", "CN", "dt", "20250811", "hour", "20"));

        // test cross partition update
        writeWithCommit(
                deltaTable, row(RowKind.UPDATE_BEFORE, 1L, 3L, "1-1", "CN", "20250811", "22"));
        writeWithCommit(
                deltaTable, row(RowKind.UPDATE_AFTER, 1L, 4L, "1-2", "CN", "20250811", "21"));
        assertThat(getResult(loadTable(), ImmutableMap.of("dt", "20250811", "hour", "22")))
                .containsExactlyInAnyOrder(
                        row(RowKind.UPDATE_AFTER, 1L, 4L, "1-2", "CN", "20250811", "22"),
                        row(RowKind.UPDATE_AFTER, 2L, 2L, "2-1", "CN", "20250811", "22"),
                        row(RowKind.UPDATE_AFTER, 3L, 2L, "3-1", "CN", "20250811", "22"));
    }

    @ParameterizedTest
    @MethodSource("dvChangelogParams")
    public void testChainTableCrossPartitionDeletionVectors(
            boolean deletionVectors, CoreOptions.ChangelogProducer changelog) throws Exception {
        createChainTable(
                options -> {
                    options.set(DELETION_VECTORS_ENABLED, deletionVectors);
                    options.set(CHANGELOG_PRODUCER, changelog);
                });
        FileStoreTable chainTable = loadTable();
        FileStoreTable snapshotTable = chainTable.switchToBranch(SNAPSHOT_BRANCH);
        writeWithCommit(snapshotTable, row(1L, 1L, "1-1", "CN", "20250811", "20"));

        FileStoreTable deltaTable = chainTable.switchToBranch(DELTA_BRANCH);
        writeWithCommit(
                deltaTable,
                row(1L, 2L, "1-2", "CN", "20250811", "21"),
                row(2L, 1L, "2-1", "CN", "20250811", "21"));
        writeWithCommit(deltaTable, row(RowKind.DELETE, 1L, 3L, null, "CN", "20250811", "21"));

        assertThat(getResult(loadTable(), ImmutableMap.of("dt", "20250811", "hour", "21")))
                .containsExactlyInAnyOrder(row(2L, 1L, "2-1", "CN", "20250811", "21"));
        assertChainSplitCarriesDeletionFiles(
                deletionVectors, ImmutableMap.of("region", "CN", "dt", "20250811", "hour", "21"));

        writeWithCommit(
                deltaTable, row(RowKind.UPDATE_BEFORE, 2L, 2L, "2-1", "CN", "20250811", "22"));
        assertThat(getResult(loadTable(), ImmutableMap.of("dt", "20250811", "hour", "22")))
                .isEmpty();
        assertChainSplitCarriesDeletionFiles(
                deletionVectors, ImmutableMap.of("region", "CN", "dt", "20250811", "hour", "22"));
    }

    private void assertChainSplitCarriesDeletionFiles(
            boolean deletionVectors, Map<String, String> partitionFilter) {
        if (!deletionVectors) {
            return;
        }
        TableScan.Plan plan =
                loadTable().newReadBuilder().withPartitionFilter(partitionFilter).newScan().plan();

        boolean foundChainSplit = false;
        for (Split split : plan.splits()) {
            if (split instanceof FallbackReadFileStoreTable.FallbackSplit) {
                foundChainSplit = true;
                ChainSplit chainSplit =
                        (ChainSplit) ((FallbackReadFileStoreTable.FallbackSplit) split).wrapped();
                Optional<List<DeletionFile>> deletionFilesOpt = chainSplit.deletionFiles();
                assertThat(deletionFilesOpt).isPresent();
                long nonNullDeletionFiles =
                        deletionFilesOpt.get().stream().filter(Objects::nonNull).count();
                assertThat(nonNullDeletionFiles)
                        .as("ChainSplit should carry deletion files from both branches")
                        .isGreaterThan(0);
            }
        }
        assertThat(foundChainSplit).as("Should have found at least one ChainSplit").isTrue();
    }

    @Test
    public void testQueryAuthBatchPlanKeepsChainSplitsWrapped() throws Exception {
        createChainTableWithQueryAuth();
        TableQueryAuthResult authResult = maskValueAndFilterKey();

        FileStoreTable table = loadTable(queryAuthEnvironment(this::maskValueAndFilterKey));
        List<Split> splits = planQueriedPartition(table.newScan());

        assertChainSplitsCarryAuth(splits, authResult);
    }

    @Test
    public void testQueryAuthScanWithSnapshotReaderFactoryKeepsChainSplitsWrapped()
            throws Exception {
        createChainTableWithQueryAuth();
        TableQueryAuthResult authResult = maskValueAndFilterKey();

        FileStoreTable table = loadTable(queryAuthEnvironment(this::maskValueAndFilterKey));
        List<Split> splits = planQueriedPartition(table.newScan(FileStoreTable::newSnapshotReader));

        assertChainSplitsCarryAuth(splits, authResult);
    }

    @Test
    public void testQueryAuthRulesAreAppliedWhenReadingChainSplits() throws Exception {
        createChainTableWithQueryAuth();

        // the same query without any rule: raw values, no row dropped
        assertThat(getResult(loadTable(), QUERIED_PARTITION))
                .containsExactlyInAnyOrder(
                        row(1L, 2L, "1-1", "CN", "20250810", "21"),
                        row(2L, 1L, "2", "CN", "20250810", "21"),
                        row(4L, 1L, "4", "CN", "20250810", "21"));

        FileStoreTable table = loadTable(queryAuthEnvironment(this::maskValueAndFilterKey));

        // 'v' comes back as the region it is masked with, and k=4 is dropped by the row filter
        assertThat(getResult(table, QUERIED_PARTITION))
                .containsExactlyInAnyOrder(
                        row(1L, 2L, "CN", "CN", "20250810", "21"),
                        row(2L, 1L, "CN", "CN", "20250810", "21"));
    }

    /**
     * A chain partition key names the partition a row is reported under, not the branch partition
     * it is stored in: k=2 sits in the snapshot branch under hour 20, and the chain merge hands it
     * to the reader under the queried hour 21. A rule on 'hour' therefore cannot prune branch
     * partitions, or the row it does admit disappears.
     */
    @Test
    public void testQueryAuthChainPartitionRuleKeepsRowsOfAnEarlierBranchPartition()
            throws Exception {
        createChainTableWithQueryAuth();

        FileStoreTable table = loadTable(queryAuthEnvironment(this::keepQueriedChainPartition));

        assertThat(getResult(table, QUERIED_PARTITION))
                .containsExactlyInAnyOrder(
                        row(1L, 2L, "1-1", "CN", "20250810", "21"),
                        row(2L, 1L, "2", "CN", "20250810", "21"),
                        row(4L, 1L, "4", "CN", "20250810", "21"));
    }

    /**
     * The other direction: a partition the snapshot branch holds in full is read from it directly,
     * branch partition and queried partition being the same row of values. A rule excluding that
     * partition still has to prune it away, not hand the reader a split it filters down to nothing.
     */
    @Test
    public void testQueryAuthPartitionRuleStillPrunesASnapshotBranchPartition() throws Exception {
        createChainTableWithQueryAuth();
        Map<String, String> snapshotPartition = ImmutableMap.of("dt", "20250810", "hour", "20");

        assertThat(planPartition(loadTable().newScan(), snapshotPartition)).isNotEmpty();

        // the rule admits hour 21 only, so nothing of the queried hour 20 is left to read
        FileStoreTable table = loadTable(queryAuthEnvironment(this::keepQueriedChainPartition));
        assertThat(planPartition(table.newScan(), snapshotPartition)).isEmpty();
        assertThat(getResult(table, snapshotPartition)).isEmpty();
    }

    /**
     * Partition listing has no read to follow it, so an excluded partition would be reported — with
     * its row and file counts — to a caller that cannot read a row of it.
     */
    @Test
    public void testQueryAuthPartitionRulePrunesTheListedPartitions() throws Exception {
        createChainTableWithQueryAuth();

        assertThat(listedHours(loadTable())).containsExactlyInAnyOrder("20", "21");

        FileStoreTable table = loadTable(queryAuthEnvironment(this::keepQueriedChainPartition));
        assertThat(listedHours(table)).containsExactly("21");
    }

    private List<String> listedHours(FileStoreTable table) {
        return table.newScan().listPartitions().stream()
                .map(partition -> partition.getString(2).toString())
                .collect(Collectors.toList());
    }

    /**
     * The authorization has not changed when the pushdown is turned off, so the cheap way out of
     * reapplying it would leave the pruning the caller just asked to drop.
     */
    @Test
    public void testQueryAuthPartitionPushdownIsDroppedAfterOneWasPushed() throws Exception {
        createChainTableWithQueryAuth();
        FileStoreTable snapshotBranch =
                loadBranchTable(
                        SNAPSHOT_BRANCH, queryAuthEnvironment(this::keepQueriedChainPartition));

        // the rule admits hour 21 and this branch holds only hour 20, so the push prunes it away
        DataTableScan scan = snapshotBranch.newScan();
        assertThat(scan.listPartitions()).isEmpty();

        assertThat(scan.withoutAuthPartitionPushdown().plan().splits()).isNotEmpty();
    }

    /** The branch as an ordinary table. switchToBranch would build another chain table. */
    private FileStoreTable loadBranchTable(String branch, CatalogEnvironment catalogEnvironment) {
        Path tablePath = new Path(tempDir.toUri().toString(), tableName);
        LocalFileIO fileIO = LocalFileIO.create();
        Optional<TableSchema> schemaOpt =
                new FileSystemSchemaManager(fileIO, tablePath, branch).latest();
        assertThat(schemaOpt.isPresent()).isTrue();
        Options options = new Options(schemaOpt.get().options());
        options.set(CHAIN_TABLE_ENABLED, false);
        options.set(CoreOptions.BRANCH, branch);
        return FileStoreTableFactory.create(
                fileIO, tablePath, schemaOpt.get().copy(options.toMap()), catalogEnvironment);
    }

    /** Keeps only the rows reported under the chain partition hour = '21'. */
    private TableQueryAuthResult keepQueriedChainPartition() {
        RowType rowType = loadTable().schema().logicalRowType();
        Predicate rowFilter = new PredicateBuilder(rowType).equal(5, BinaryString.fromString("21"));
        return new TableQueryAuthResult(
                Collections.singletonList(JsonSerdeUtil.toFlatJson(rowFilter)), null);
    }

    @Test
    public void testQueryAuthWithoutRulesLeavesPlanAndReadUntouched() throws Exception {
        createChainTableWithQueryAuth();
        List<GenericRow> withoutQueryAuth = getResult(loadTable(), QUERIED_PARTITION);

        // authorization answers with neither a row filter nor a column mask
        FileStoreTable table =
                loadTable(queryAuthEnvironment(() -> new TableQueryAuthResult(null, null)));
        List<Split> splits = planQueriedPartition(table.newScan());

        assertThat(splits).isNotEmpty();
        assertThat(splits)
                .allSatisfy(
                        split ->
                                assertThat(
                                                ((FallbackReadFileStoreTable.FallbackSplit) split)
                                                        .wrapped())
                                        .isInstanceOf(ChainSplit.class));
        assertThat(getResult(table, QUERIED_PARTITION))
                .containsExactlyInAnyOrderElementsOf(withoutQueryAuth);
    }

    @Test
    public void testQueryAuthStreamingStartingPlanKeepsChainSplitsWrapped() throws Exception {
        createChainTableWithQueryAuth();
        TableQueryAuthResult authResult = maskValueAndFilterKey();

        FileStoreTable table = loadTable(queryAuthEnvironment(this::maskValueAndFilterKey));
        List<Split> splits = table.newStreamScan().plan().splits();

        assertThat(splits).isNotEmpty();
        assertThat(splits)
                .allSatisfy(
                        split -> {
                            assertThat(split).isInstanceOf(QueryAuthSplit.class);
                            QueryAuthSplit authSplit = (QueryAuthSplit) split;
                            assertThat(authSplit.authResult()).isEqualTo(authResult);
                            assertThat(authSplit.split()).isInstanceOf(ChainSplit.class);
                        });

        // phase 1 reads the latest snapshot partition plus the later delta partitions, so the
        // same key shows up under both; what matters here is that both carry the rules
        List<GenericRow> rows = readSplits(table, splits);
        assertThat(rows).isNotEmpty();
        assertThat(rows)
                .allSatisfy(
                        r -> {
                            assertThat(r.getLong(0)).isLessThan(3L);
                            assertThat(r.getString(2)).isEqualTo(r.getString(3));
                        });
    }

    private static final Map<String, String> QUERIED_PARTITION =
            ImmutableMap.of("dt", "20250810", "hour", "21");

    private void createChainTableWithQueryAuth() throws Exception {
        createChainTable(
                options -> {
                    options.set(CoreOptions.QUERY_AUTH_ENABLED, true);
                    // a chain read that cannot handle its own split otherwise falls through to the
                    // main branch with nothing but a log line, which would hide a routing mistake
                    options.set(CoreOptions.SCAN_FALLBACK_BRANCH_READ_FAIL_FAST, true);
                });
        FileStoreTable chainTable = loadTable();
        writeWithCommit(
                chainTable.switchToBranch(SNAPSHOT_BRANCH),
                row(1L, 1L, "1", "CN", "20250810", "20"),
                row(2L, 1L, "2", "CN", "20250810", "20"));
        writeWithCommit(
                chainTable.switchToBranch(DELTA_BRANCH),
                row(1L, 2L, "1-1", "CN", "20250810", "21"),
                row(4L, 1L, "4", "CN", "20250810", "21"));
    }

    /** Masks column 'v' with the value of column 'region' and keeps only the rows with k &lt; 3. */
    private TableQueryAuthResult maskValueAndFilterKey() {
        RowType rowType = loadTable().schema().logicalRowType();
        Predicate rowFilter = new PredicateBuilder(rowType).lessThan(0, 3L);
        Transform mask = new FieldTransform(new FieldRef(3, "region", DataTypes.STRING()));
        return new TableQueryAuthResult(
                Collections.singletonList(JsonSerdeUtil.toFlatJson(rowFilter)),
                Collections.singletonMap("v", JsonSerdeUtil.toFlatJson(mask)));
    }

    /**
     * A catalog environment whose authorization answers with {@code authResult}. The table, its
     * branch scans and its reads are all real; only the catalog call fetching the rules is stubbed.
     * {@link CatalogEnvironment#empty()} cannot express this: without a catalog loader it short
     * circuits to "no rules", so no split is ever wrapped.
     */
    private CatalogEnvironment queryAuthEnvironment(Supplier<TableQueryAuthResult> authResult) {
        Catalog catalog = Mockito.mock(Catalog.class);
        try {
            // a fresh instance per call, as RESTCatalog#authTableQuery builds one per scan
            Mockito.when(catalog.authTableQuery(Mockito.any(), Mockito.any()))
                    .thenAnswer(invocation -> authResult.get());
            // the snapshot manager takes the same catalog; let it fall back to the file system
            // instead of answering "no snapshot" for every branch
            Mockito.when(catalog.loadSnapshot(Mockito.any(Identifier.class)))
                    .thenThrow(new UnsupportedOperationException());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return new CatalogEnvironment(
                Identifier.create("default", tableName),
                null,
                () -> catalog,
                null,
                null,
                null,
                false,
                false);
    }

    private List<Split> planQueriedPartition(DataTableScan scan) {
        return planPartition(scan, QUERIED_PARTITION);
    }

    private List<Split> planPartition(DataTableScan scan, Map<String, String> partition) {
        return scan.withPartitionFilter(partition).plan().splits();
    }

    private void assertChainSplitsCarryAuth(List<Split> splits, TableQueryAuthResult authResult) {
        assertThat(splits).isNotEmpty();
        assertThat(splits)
                .allSatisfy(
                        split -> {
                            assertThat(split)
                                    .isInstanceOf(FallbackReadFileStoreTable.FallbackSplit.class);
                            Split wrapped =
                                    ((FallbackReadFileStoreTable.FallbackSplit) split).wrapped();
                            assertThat(wrapped).isInstanceOf(QueryAuthSplit.class);
                            QueryAuthSplit authSplit = (QueryAuthSplit) wrapped;
                            assertThat(authSplit.authResult()).isEqualTo(authResult);
                            assertThat(authSplit.split()).isInstanceOf(ChainSplit.class);
                        });
    }

    private List<GenericRow> readSplits(FileStoreTable table, List<Split> splits) throws Exception {
        InternalRowSerializer serializer =
                new InternalRowSerializer(table.schema().logicalRowType());
        InnerTableRead read = table.newRead();
        List<GenericRow> result = new ArrayList<>();
        for (Split split : splits) {
            try (RecordReader<InternalRow> reader = read.createReader(split)) {
                reader.forEachRemaining(row -> result.add((GenericRow) serializer.copy(row)));
            }
        }
        return result;
    }

    @Test
    public void testChainOverwriteClearsSnapshotPartition() throws Exception {
        createChainTable(options -> {});
        FileStoreTable chainTable = loadTable();
        FileStoreTable snapshotTable = chainTable.switchToBranch(SNAPSHOT_BRANCH);
        FileStoreTable deltaTable = chainTable.switchToBranch(DELTA_BRANCH);
        Map<String, String> partition = partition("20");

        writeWithCommit(snapshotTable, row(1L, 1L, "value-1", "CN", "20250810", "20"));
        assertThat(getResult(loadTable(), partition))
                .containsExactly(row(1L, 1L, "value-1", "CN", "20250810", "20"));

        // An overwrite of the delta branch clears the same partition of the snapshot branch, so
        // that reads fall through to the delta.
        commitOverwrite(deltaTable, partition, 0, row(1L, 2L, "value-2", "CN", "20250810", "20"));
        assertThat(getResult(loadTable(), partition))
                .containsExactly(row(1L, 2L, "value-2", "CN", "20250810", "20"));
    }

    @Test
    public void testChainOverwriteReplayCompletesSnapshotCleanup() throws Exception {
        createChainTable(options -> {});
        FileStoreTable chainTable = loadTable();
        FileStoreTable snapshotTable = chainTable.switchToBranch(SNAPSHOT_BRANCH);
        FileStoreTable deltaTable = chainTable.switchToBranch(DELTA_BRANCH);
        Map<String, String> partition = partition("20");

        writeWithCommit(snapshotTable, row(1L, 1L, "value-1", "CN", "20250810", "20"));

        // The overwrite publishes its snapshot, and only then clears the snapshot branch. Make
        // that cleanup fail, the way a transient failure of the snapshot branch would: the delta
        // snapshot is published, the commit throws, and the partition is not cleared.
        assertThatThrownBy(
                        () ->
                                commitOverwrite(
                                        withFailingCleanup(deltaTable),
                                        partition,
                                        0,
                                        row(1L, 2L, "value-2", "CN", "20250810", "20")))
                .hasMessageContaining("does not exist");
        assertThat(deltaTable.snapshotManager().snapshotCount())
                .as("the delta snapshot was published before the cleanup failed")
                .isEqualTo(1);
        assertThat(getResult(loadTable(), partition))
                .as("the snapshot branch still hides the delta until its partition is cleared")
                .containsExactly(row(1L, 1L, "value-1", "CN", "20250810", "20"));

        // A restarted job replays the batch under the same user and identifier. The commit is
        // recognised as already published, so the callback is retried rather than called, and
        // the retry has to complete the cleanup the first attempt did not.
        commitOverwrite(deltaTable, partition, 0, row(1L, 2L, "value-2", "CN", "20250810", "20"));

        assertThat(deltaTable.snapshotManager().snapshotCount())
                .as("the replay must not publish a second snapshot")
                .isEqualTo(1);
        assertThat(getResult(loadTable(), partition))
                .as("the replay must complete the cleanup of the snapshot branch")
                .containsExactly(row(1L, 2L, "value-2", "CN", "20250810", "20"));
    }

    @Test
    public void testChainOverwriteReplayClearsPartitionWithOnlyRemovedFiles() throws Exception {
        createChainTable(options -> {});
        FileStoreTable chainTable = loadTable();
        FileStoreTable snapshotTable = chainTable.switchToBranch(SNAPSHOT_BRANCH);
        FileStoreTable deltaTable = chainTable.switchToBranch(DELTA_BRANCH);

        writeWithCommit(
                snapshotTable,
                row(1L, 1L, "value-1", "CN", "20250810", "20"),
                row(2L, 1L, "value-1", "CN", "20250810", "21"));
        writeWithCommit(deltaTable, row(1L, 1L, "delta-old", "CN", "20250810", "20"));

        // A static overwrite of the whole delta branch that writes only to hour 21 removes the
        // file of hour 20 without adding one there. The cleanup has to cover hour 20 as well,
        // which the commit messages of the batch do not mention; only the manifest changes of the
        // snapshot do. (A dynamic partition overwrite, the default, would leave hour 20 alone.)
        Map<String, String> staticOverwrite = new HashMap<>();
        staticOverwrite.put(CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key(), "false");
        FileStoreTable staticDelta = deltaTable.copy(staticOverwrite);
        assertThatThrownBy(
                        () ->
                                commitOverwrite(
                                        withFailingCleanup(staticDelta),
                                        Collections.emptyMap(),
                                        0,
                                        row(2L, 2L, "value-2", "CN", "20250810", "21")))
                .hasMessageContaining("does not exist");
        commitOverwrite(
                staticDelta,
                Collections.emptyMap(),
                0,
                row(2L, 2L, "value-2", "CN", "20250810", "21"));

        assertThat(deltaTable.snapshotManager().snapshotCount()).isEqualTo(2);
        assertThat(getResult(loadTable(), partition("20")))
                .as("a partition the overwrite only removed files from must be cleared too")
                .isEmpty();
        assertThat(getResult(loadTable(), partition("21")))
                .containsExactly(row(2L, 2L, "value-2", "CN", "20250810", "21"));
    }

    @Test
    public void testChainOverwriteReplayKeepsSnapshotDataWrittenAfterTheOverwrite()
            throws Exception {
        createChainTable(options -> {});
        FileStoreTable chainTable = loadTable();
        FileStoreTable snapshotTable = chainTable.switchToBranch(SNAPSHOT_BRANCH);
        FileStoreTable deltaTable = chainTable.switchToBranch(DELTA_BRANCH);
        Map<String, String> partition = partition("20");

        writeWithCommit(snapshotTable, row(1L, 1L, "value-1", "CN", "20250810", "20"));
        // The overwrite and its cleanup both succeed.
        commitOverwrite(deltaTable, partition, 0, row(1L, 2L, "value-2", "CN", "20250810", "20"));
        assertThat(getResult(loadTable(), partition))
                .containsExactly(row(1L, 2L, "value-2", "CN", "20250810", "20"));

        // New data lands in the same partition of the snapshot branch afterwards and, being the
        // snapshot, takes precedence over the delta.
        writeWithCommit(snapshotTable, row(2L, 1L, "value-3", "CN", "20250810", "20"));
        assertThat(getResult(loadTable(), partition))
                .containsExactly(row(2L, 1L, "value-3", "CN", "20250810", "20"));

        // The batch is replayed although its callback had completed, for example because the
        // engine failed after the sink returned. The retry must not clear what landed later.
        commitOverwrite(deltaTable, partition, 0, row(1L, 2L, "value-2", "CN", "20250810", "20"));

        assertThat(getResult(snapshotTable, partition))
                .as(
                        "data written to the snapshot branch after the overwrite must survive its replay")
                .containsExactly(row(2L, 1L, "value-3", "CN", "20250810", "20"));
        assertThat(getResult(loadTable(), partition))
                .containsExactly(row(2L, 1L, "value-3", "CN", "20250810", "20"));
    }

    @Test
    public void testChainOverwriteReplayClearsOnlyWhatTheOverwriteSuperseded() throws Exception {
        createChainTable(options -> {});
        FileStoreTable chainTable = loadTable();
        FileStoreTable snapshotTable = chainTable.switchToBranch(SNAPSHOT_BRANCH);
        FileStoreTable deltaTable = chainTable.switchToBranch(DELTA_BRANCH);
        Map<String, String> partition = partition("20");

        writeWithCommit(snapshotTable, row(1L, 1L, "value-1", "CN", "20250810", "20"));
        // The overwrite is published, its cleanup fails.
        assertThatThrownBy(
                        () ->
                                commitOverwrite(
                                        withFailingCleanup(deltaTable),
                                        partition,
                                        0,
                                        row(1L, 2L, "value-2", "CN", "20250810", "20")))
                .hasMessageContaining("does not exist");

        // New data lands in the same partition of the snapshot branch before the replay.
        writeWithCommit(snapshotTable, row(2L, 1L, "value-3", "CN", "20250810", "20"));

        // Had the cleanup run when the overwrite was published, it would have cleared value-1
        // and left value-3, which came later, in place. The retry has to do the same.
        commitOverwrite(deltaTable, partition, 0, row(1L, 2L, "value-2", "CN", "20250810", "20"));

        assertThat(getResult(snapshotTable, partition))
                .as(
                        "the retry must clear what the overwrite superseded and nothing that came later")
                .containsExactly(row(2L, 1L, "value-3", "CN", "20250810", "20"));
        assertThat(getResult(loadTable(), partition))
                .containsExactly(row(2L, 1L, "value-3", "CN", "20250810", "20"));
    }

    @Test
    public void testChainOverwriteReplayWithoutSnapshotIsSkipped() throws Exception {
        createChainTable(options -> {});
        FileStoreTable deltaTable = loadTable().switchToBranch(DELTA_BRANCH);
        Map<String, String> partition = partition("20");

        commitOverwrite(deltaTable, partition, 0, row(1L, 1L, "value-1", "CN", "20250810", "20"));
        commitOverwrite(deltaTable, partition, 2, row(1L, 2L, "value-2", "CN", "20250810", "20"));
        long snapshots = deltaTable.snapshotManager().snapshotCount();

        // Identifier 1 is below the latest committed one, so it is treated as a replay and
        // retried, but no snapshot carries it. That is not an error: nothing is left to clean.
        commitOverwrite(deltaTable, partition, 1, row(1L, 3L, "value-3", "CN", "20250810", "20"));

        assertThat(deltaTable.snapshotManager().snapshotCount()).isEqualTo(snapshots);
        assertThat(getResult(loadTable(), partition))
                .containsExactly(row(1L, 2L, "value-2", "CN", "20250810", "20"));
    }

    private static Map<String, String> partition(String hour) {
        return ImmutableMap.of("region", "CN", "dt", "20250810", "hour", hour);
    }

    /**
     * The delta table with the snapshot branch cleanup of an overwrite failing after the delta
     * snapshot is published, since the branch it is told to clean does not exist.
     */
    private static FileStoreTable withFailingCleanup(FileStoreTable deltaTable) {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.SCAN_FALLBACK_SNAPSHOT_BRANCH.key(), "unreachable");
        return deltaTable.copy(options);
    }

    private void commitOverwrite(
            FileStoreTable deltaTable,
            Map<String, String> overwrite,
            long identifier,
            GenericRow... rows)
            throws Exception {
        BatchWriteBuilderImpl builder =
                ((BatchWriteBuilderImpl) deltaTable.newBatchWriteBuilder()).withCommitUser("user");
        builder.withOverwrite(overwrite);
        try (BatchTableWrite write =
                        builder.newWrite().withIOManager(new IOManagerImpl(tempDir.toString()));
                InnerTableCommit commit = builder.newCommit()) {
            for (GenericRow r : rows) {
                write.write(r);
            }
            commit.filterAndCommit(Collections.singletonMap(identifier, write.prepareCommit()));
        }
    }

    private FileStoreTable loadTable() {
        return loadTable(CatalogEnvironment.empty());
    }

    private FileStoreTable loadTable(CatalogEnvironment catalogEnvironment) {
        Path tablePath = new Path(tempDir.toUri().toString(), tableName);
        LocalFileIO fileIO = LocalFileIO.create();
        Options options = new Options();
        options.set(CoreOptions.PATH, tablePath.toString());
        String branchName = CoreOptions.branch(options.toMap());
        Optional<TableSchema> schemaOpt =
                new FileSystemSchemaManager(fileIO, tablePath, branchName).latest();
        assertThat(schemaOpt.isPresent()).isTrue();
        return FileStoreTableFactory.create(fileIO, tablePath, schemaOpt.get(), catalogEnvironment);
    }

    private void createChainTable(Consumer<Options> optionCustomizer) throws Exception {
        Path tablePath = new Path(tempDir.toUri().toString(), tableName);
        LocalFileIO fileIO = LocalFileIO.create();
        SchemaManager mainSchemaManager = new FileSystemSchemaManager(fileIO, tablePath);

        Options options = new Options();
        options.set(BUCKET, 1);
        options.set(BUCKET_KEY, "k");
        options.set(SEQUENCE_FIELD, "seq");
        options.set(MERGE_ENGINE, CoreOptions.MergeEngine.DEDUPLICATE);
        options.set(CHAIN_TABLE_ENABLED, true);
        options.set(PARTITION_TIMESTAMP_PATTERN, "$dt $hour:00:00");
        options.set(PARTITION_TIMESTAMP_FORMATTER, "yyyyMMdd HH:mm:ss");
        options.set(CHAIN_TABLE_CHAIN_PARTITION_KEYS, "dt,hour");
        options.set(PATH, tablePath.toString());
        optionCustomizer.accept(options);

        Schema schema =
                new Schema(
                        RowType.of(
                                        new org.apache.paimon.types.DataType[] {
                                            DataTypes.BIGINT(),
                                            DataTypes.BIGINT(),
                                            DataTypes.STRING(),
                                            DataTypes.STRING(),
                                            DataTypes.STRING(),
                                            DataTypes.STRING()
                                        },
                                        new String[] {"k", "seq", "v", "region", "dt", "hour"})
                                .getFields(),
                        Arrays.asList("region", "dt", "hour"),
                        Arrays.asList("region", "dt", "hour", "k"),
                        options.toMap(),
                        "");

        mainSchemaManager.createTable(schema);
        FileStoreTable table = loadTable();

        // chain table setup procedure.
        table.createBranch(SNAPSHOT_BRANCH);
        table.createBranch(DELTA_BRANCH);

        configureBranchOptions(fileIO, tablePath, DEFAULT_MAIN_BRANCH);
        configureBranchOptions(fileIO, tablePath, SNAPSHOT_BRANCH);
        configureBranchOptions(fileIO, tablePath, DELTA_BRANCH);

        Optional<TableSchema> schemaOpt = mainSchemaManager.latest();
        assertThat(schemaOpt.isPresent()).isTrue();
    }

    private void createPartialUpdateChainTable(Consumer<Options> optionCustomizer)
            throws Exception {
        Path tablePath = new Path(tempDir.toUri().toString(), tableName);
        LocalFileIO fileIO = LocalFileIO.create();
        SchemaManager mainSchemaManager = new FileSystemSchemaManager(fileIO, tablePath);

        Options options = new Options();
        options.set(BUCKET, 1);
        options.set(BUCKET_KEY, "k");
        options.set(SEQUENCE_FIELD, "seq");
        options.set(MERGE_ENGINE, CoreOptions.MergeEngine.PARTIAL_UPDATE);
        options.set(CHAIN_TABLE_ENABLED, true);
        options.set(PARTITION_TIMESTAMP_PATTERN, "$dt");
        options.set(PARTITION_TIMESTAMP_FORMATTER, "yyyyMMdd");
        options.set(CHAIN_TABLE_CHAIN_PARTITION_KEYS, "dt");
        options.set(PATH, tablePath.toString());
        optionCustomizer.accept(options);

        Schema schema =
                new Schema(
                        RowType.of(
                                        new org.apache.paimon.types.DataType[] {
                                            DataTypes.BIGINT(),
                                            DataTypes.BIGINT(),
                                            DataTypes.STRING(),
                                            DataTypes.STRING(),
                                            DataTypes.STRING()
                                        },
                                        new String[] {"k", "seq", "v1", "v2", "dt"})
                                .getFields(),
                        Collections.singletonList("dt"),
                        Arrays.asList("dt", "k"),
                        options.toMap(),
                        "");

        mainSchemaManager.createTable(schema);
        FileStoreTable table = loadTable();

        table.createBranch(SNAPSHOT_BRANCH);
        table.createBranch(DELTA_BRANCH);

        configureBranchOptions(fileIO, tablePath, DEFAULT_MAIN_BRANCH);
        configureBranchOptions(fileIO, tablePath, SNAPSHOT_BRANCH);
        configureBranchOptions(fileIO, tablePath, DELTA_BRANCH);

        Optional<TableSchema> schemaOpt = mainSchemaManager.latest();
        assertThat(schemaOpt.isPresent()).isTrue();
    }

    private void configureBranchOptions(LocalFileIO fileIO, Path tablePath, String branchName)
            throws Exception {
        SchemaManager branchSchemaManager =
                new FileSystemSchemaManager(fileIO, tablePath, branchName);
        branchSchemaManager.commitChanges(
                SchemaChange.setOption(
                        CoreOptions.SCAN_FALLBACK_SNAPSHOT_BRANCH.key(), SNAPSHOT_BRANCH),
                SchemaChange.setOption(CoreOptions.SCAN_FALLBACK_DELTA_BRANCH.key(), DELTA_BRANCH));
    }

    private GenericRow row(RowKind kind, Object... values) {
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null && values[i] instanceof String) {
                values[i] = BinaryString.fromString((String) values[i]);
            }
        }
        return GenericRow.ofKind(kind, values);
    }

    private GenericRow row(Object... values) {
        return row(RowKind.INSERT, values);
    }

    private List<GenericRow> getResult(FileStoreTable table, Map<String, String> partitionFilter)
            throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.withPartitionFilter(partitionFilter).newScan().plan();
        List<GenericRow> result = new ArrayList<>();
        RowType rowType = table.schema().logicalRowType();
        InternalRowSerializer serializer = new InternalRowSerializer(rowType);

        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
            reader.forEachRemaining(row -> result.add((GenericRow) serializer.copy(row)));
        }
        return result;
    }

    private void writeWithCommit(FileStoreTable table, GenericRow... rows) throws Exception {
        try (InnerTableWrite write =
                table.newWrite(commitUser).withIOManager(new IOManagerImpl(tempDir.toString()))) {
            for (GenericRow r : rows) {
                write.write(r);
            }
            try (StreamTableCommit commit = table.newCommit(commitUser)) {
                List<CommitMessage> messages = write.prepareCommit(true, 0);
                commit.commit(0, messages);
            }
        }
    }
}
