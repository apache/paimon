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

package org.apache.paimon.append.cluster;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactTask;
import org.apache.paimon.append.dataevolution.DataEvolutionNormalCompactTask;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.stats.SimpleStats.EMPTY_STATS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Compaction fallback for accumulated data-evolution column versions. */
class DataEvolutionClusterCompactionTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testThresholdIncludesEveryVersionAndPreservesEachGroup() throws Exception {
        FileStoreTable table = createTable();
        List<DataFileMeta> files = versions(table, 0, 5);
        files.addAll(versions(table, 3, 4));
        List<DataEvolutionCompactTask> tasks = plan(table, files, Collections.emptyMap());
        assertThat(tasks).hasSize(1);
        assertThat(tasks.get(0).compactBefore())
                .containsExactlyInAnyOrderElementsOf(files.subList(0, 5));
        assertThat(((DataEvolutionNormalCompactTask) tasks.get(0)).outputLevel()).isEqualTo(2);

        files.add(file(table, "fifth-second-group", 3, 30, 0, "payload"));
        tasks = plan(table, files, Collections.emptyMap());
        assertThat(tasks).hasSize(2);
        assertThat(tasks)
                .allSatisfy(
                        task -> {
                            assertThat(task.compactBefore()).hasSize(5);
                            assertThat(task.compactBefore())
                                    .extracting(DataFileMeta::nonNullRowIdRange)
                                    .containsOnly(task.compactBefore().get(0).nonNullRowIdRange());
                        });
    }

    @Test
    void testSingleFileIsNotRewrittenEvenWithLowThreshold() throws Exception {
        FileStoreTable table =
                createTable()
                        .copy(
                                Collections.singletonMap(
                                        CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "1"));
        assertThat(plan(table, versions(table, 0, 1), Collections.emptyMap())).isEmpty();
    }

    @Test
    void testClusteringSelectedGroupIsNotCompactedAgain() throws Exception {
        FileStoreTable table = createTable();
        List<DataFileMeta> files = versions(table, 0, 5);
        List<DataFileMeta> secondGroup = versions(table, 3, 5);
        files.addAll(secondGroup);
        Map<BinaryRow, CompactUnit> clustering =
                Collections.singletonMap(
                        BinaryRow.EMPTY_ROW, CompactUnit.fromFiles(2, files.subList(0, 5), true));
        List<DataEvolutionCompactTask> tasks = plan(table, files, clustering);
        assertThat(tasks).hasSize(1);
        assertThat(tasks.get(0).compactBefore()).containsExactlyInAnyOrderElementsOf(secondGroup);
    }

    @Test
    void testKeyUpdatesAndChangedDefinitionsDoNotInheritLevel() throws Exception {
        FileStoreTable table = createTable();
        List<DataFileMeta> keyUpdated = versions(table, 0, 5);
        keyUpdated.add(file(table, "key-update", 0, 30, 0, "id"));
        assertUnsortedCompaction(plan(table, keyUpdated, Collections.emptyMap()), keyUpdated);

        List<DataFileMeta> versions = versions(table, 0, 5);
        for (String[] definition : new String[][] {{"payload", "order"}, {"id", "zorder"}}) {
            FileStoreTable changed =
                    table.copy(
                            table.schemaManager()
                                    .commitChanges(
                                            Arrays.asList(
                                                    SchemaChange.setOption(
                                                            CoreOptions.CLUSTERING_COLUMNS.key(),
                                                            definition[0]),
                                                    SchemaChange.setOption(
                                                            CoreOptions.CLUSTERING_STRATEGY.key(),
                                                            definition[1]))));
            assertUnsortedCompaction(plan(changed, versions, Collections.emptyMap()), versions);
        }
        FileStoreTable newKey =
                table.copy(
                        table.schemaManager()
                                .commitChanges(
                                        Arrays.asList(
                                                SchemaChange.addColumn("new_key", DataTypes.INT()),
                                                SchemaChange.setOption(
                                                        CoreOptions.CLUSTERING_COLUMNS.key(),
                                                        "new_key"))));
        assertUnsortedCompaction(plan(newKey, versions, Collections.emptyMap()), versions);
    }

    @Test
    void testUnsortedOrIncompleteAnchorDoesNotInheritLevel() throws Exception {
        FileStoreTable table = createTable();
        List<DataFileMeta> unsorted = versions(table, 0, 5);
        unsorted.set(0, file(table, "unsorted-anchor", 0, 10, 0, null));
        assertUnsortedCompaction(plan(table, unsorted, Collections.emptyMap()), unsorted);

        List<DataFileMeta> incomplete = versions(table, 0, 5);
        incomplete.set(0, file(table, "partial-anchor", 0, 10, 2, "payload"));
        assertUnsortedCompaction(plan(table, incomplete, Collections.emptyMap()), incomplete);
    }

    @Test
    void testSingleDirtyRunSkippedByMinorStillCompactsColumnVersions() throws Exception {
        FileStoreTable table = createTable();
        List<DataFileMeta> files = versions(table, 0, 5);
        files.set(4, file(table, "key-update", 0, 30, 0, "id"));
        IncrementalClusterStrategy strategy =
                new IncrementalClusterStrategy(
                        table.schemaManager(), table.coreOptions().clusteringColumns(), 25, 1, 2);
        assertThat(
                        strategy.pick(
                                3,
                                new DataEvolutionIncrementalClusterRewriter(table, null)
                                        .constructPartitionLevels(files),
                                false))
                .isEmpty();

        assertUnsortedCompaction(plan(table, files, Collections.emptyMap()), files);
    }

    private static void assertUnsortedCompaction(
            List<DataEvolutionCompactTask> tasks, List<DataFileMeta> input) {
        assertThat(tasks).hasSize(1);
        assertThat(tasks.get(0).compactBefore()).containsExactlyInAnyOrderElementsOf(input);
        assertThat(((DataEvolutionNormalCompactTask) tasks.get(0)).outputLevel()).isZero();
    }

    private List<DataEvolutionCompactTask> plan(
            FileStoreTable table,
            List<DataFileMeta> files,
            Map<BinaryRow, CompactUnit> clustering) {
        FileStoreTable input = spy(table);
        Snapshot snapshot = mock(Snapshot.class);
        PartitionPredicate predicate = mock(PartitionPredicate.class);
        SnapshotReader reader = mock(SnapshotReader.class);
        SnapshotReader.Plan plan = mock(SnapshotReader.Plan.class);
        doReturn(reader).when(input).newSnapshotReader();
        when(reader.dropStats()).thenReturn(reader);
        when(reader.withSnapshot(snapshot)).thenReturn(reader);
        when(reader.withPartitionFilter(predicate)).thenReturn(reader);
        when(reader.read()).thenReturn(plan);
        // Scan splits may cut between physical column versions. Planning must regroup them.
        List<DataSplit> splits = new ArrayList<>();
        for (DataFileMeta file : files) {
            splits.add(
                    DataSplit.builder()
                            .withPartition(BinaryRow.EMPTY_ROW)
                            .withBucket(0)
                            .withBucketPath("bucket-0")
                            .withDataFiles(Collections.singletonList(file))
                            .build());
        }
        when(plan.dataSplits()).thenReturn(splits);
        List<DataEvolutionCompactTask> result =
                DataEvolutionClusterCompaction.plan(input, snapshot, predicate, clustering);
        verify(reader).withSnapshot(snapshot);
        verify(reader).withPartitionFilter(predicate);
        return result;
    }

    private FileStoreTable createTable() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("payload", DataTypes.STRING())
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.CLUSTERING_INCREMENTAL.key(), "true")
                        .option(CoreOptions.CLUSTERING_COLUMNS.key(), "id")
                        .option(CoreOptions.NUM_LEVELS.key(), "3")
                        .option(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "5")
                        .build();
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(tempDir.toString());
        SchemaManager schemaManager = new FileSystemSchemaManager(fileIO, path);
        return FileStoreTableFactory.create(fileIO, path, schemaManager.createTable(schema));
    }

    private static List<DataFileMeta> versions(FileStoreTable table, long firstRowId, int count) {
        List<DataFileMeta> files = new ArrayList<>();
        files.add(file(table, "base-" + firstRowId, firstRowId, 10, 2, null));
        for (int i = 1; i < count; i++) {
            files.add(
                    file(
                            table,
                            "update-" + firstRowId + "-" + i,
                            firstRowId,
                            10 + i,
                            0,
                            "payload"));
        }
        return files;
    }

    private static DataFileMeta file(
            FileStoreTable table,
            String name,
            long firstRowId,
            long sequence,
            int level,
            String writeColumn) {
        DataFileMeta file =
                DataFileMeta.forAppend(
                        name + ".parquet",
                        100,
                        3,
                        EMPTY_STATS,
                        sequence,
                        sequence,
                        table.schema().id(),
                        Collections.emptyList(),
                        null,
                        FileSource.APPEND,
                        null,
                        null,
                        firstRowId,
                        writeColumn == null ? null : Collections.singletonList(writeColumn));
        return level == 0 ? file : file.upgrade(level);
    }
}
