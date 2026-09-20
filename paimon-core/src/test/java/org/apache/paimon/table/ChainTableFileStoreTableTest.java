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
import org.apache.paimon.options.Options;
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
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.InnerTableWrite;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.source.ChainSplit;
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
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
