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
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.stats.SimpleStats.EMPTY_STATS;
import static org.assertj.core.api.Assertions.assertThat;

/** Planner behavior for data-evolution column versions. */
class DataEvolutionIncrementalClusterRewriterTest {

    private static final int NUM_LEVELS = 3;
    private static final int MAX_LEVEL = NUM_LEVELS - 1;

    @TempDir java.nio.file.Path tempDir;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testNonKeyVersionsStayClusteredUntilKeyIsWritten(boolean fullSchemaUpdate)
            throws Exception {
        FileStoreTable original =
                createTable(
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("payload", DataTypes.STRING()),
                        "id",
                        "auto");
        FileStoreTable table =
                evolve(
                        original,
                        SchemaChange.setOption(CoreOptions.CLUSTERING_STRATEGY.key(), "order"));
        List<DataFileMeta> files = new ArrayList<>();
        for (long firstRowId : new long[] {0, 3}) {
            files.add(file(original, "base-" + firstRowId, firstRowId, 10, MAX_LEVEL, null));
            files.add(
                    file(
                                    table,
                                    "payload-" + firstRowId,
                                    firstRowId,
                                    20,
                                    0,
                                    Collections.singletonList("payload"))
                            .withWriteColsSequences(new long[] {20}));
        }

        // "auto" and "order" resolve to the same strategy for a single key. Non-key versions
        // neither create extra runs nor make a full cluster rewrite already sorted rows.
        assertThat(pick(table, files, true)).isEmpty();
        assertThat(pick(table, files, false)).isEmpty();

        files.add(
                file(
                        table,
                        "key-update",
                        0,
                        30,
                        0,
                        fullSchemaUpdate ? null : Collections.singletonList("id")));
        assertSelectedFiles(pick(table, files, true), files);
        assertSelectedFiles(pick(table, files, false), files);
    }

    @Test
    void testAnchorMissingKeyRequiresClustering() throws Exception {
        FileStoreTable table =
                createTable(
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("payload", DataTypes.STRING()),
                        "id",
                        "order");
        List<DataFileMeta> files =
                Arrays.asList(
                        file(
                                table,
                                "partial-anchor",
                                0,
                                10,
                                MAX_LEVEL,
                                Collections.singletonList("payload")),
                        file(
                                table,
                                "later-key",
                                0,
                                20,
                                MAX_LEVEL,
                                Collections.singletonList("id")));

        // Neither file has a lower level, but the initial partial rows were not sorted by id.
        assertSelectedFiles(pick(table, files, true), files);
    }

    @Test
    void testOlderSchemaCompactFileDoesNotWriteNewKey() throws Exception {
        FileStoreTable original =
                createTable(
                        Schema.newBuilder().column("payload", DataTypes.STRING()),
                        "payload",
                        "order");
        FileStoreTable table =
                evolve(
                        original,
                        SchemaChange.addColumn("id", DataTypes.INT()),
                        SchemaChange.setOption(CoreOptions.CLUSTERING_COLUMNS.key(), "id"));
        DataFileMeta anchor = file(table, "new-key-anchor", 0, 10, MAX_LEVEL, null);
        DataFileMeta compactedPayload =
                file(original, "old-schema-compact", 0, 20, 0, null)
                        .assignFileSource(FileSource.COMPACT)
                        .withWriteColsSequences(new long[] {20});

        // Null write-cols means this file's physical schema, not the current schema. A compact
        // output from an older schema cannot write a key which was introduced later.
        assertThat(pick(table, Arrays.asList(compactedPayload, anchor), true)).isEmpty();
    }

    @ParameterizedTest
    @CsvSource({"payload, order", "id, zorder"})
    void testFirstCurrentGroupCannotHideDifferentClusteringDefinition(
            String oldKey, String oldStrategy) throws Exception {
        FileStoreTable original =
                createTable(
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("payload", DataTypes.STRING()),
                        oldKey,
                        oldStrategy);
        FileStoreTable table =
                evolve(
                        original,
                        SchemaChange.setOption(CoreOptions.CLUSTERING_COLUMNS.key(), "id"),
                        SchemaChange.setOption(CoreOptions.CLUSTERING_STRATEGY.key(), "order"));
        List<DataFileMeta> files =
                Arrays.asList(
                        file(table, "current-definition", 0, 10, MAX_LEVEL, null),
                        file(original, "old-definition", 3, 10, MAX_LEVEL, null));

        // The shared full planner checks the first file's schema for a single highest-level run.
        // Each group's provenance must survive grouping even when a current-schema group is first.
        assertSelectedFiles(pick(table, files, true), files);
    }

    @Test
    void testPartitionOnlyKeyDoesNotRequirePhysicalColumn() throws Exception {
        FileStoreTable table =
                createTable(
                        Schema.newBuilder()
                                .column("pt", DataTypes.STRING())
                                .column("payload", DataTypes.STRING())
                                .partitionKeys("pt"),
                        "pt",
                        "order");
        List<DataFileMeta> files =
                Arrays.asList(
                        file(
                                table,
                                "partition-anchor",
                                0,
                                10,
                                MAX_LEVEL,
                                Collections.singletonList("payload")),
                        file(
                                table,
                                "payload-update",
                                0,
                                20,
                                0,
                                Collections.singletonList("payload")));

        // A partition key is constant and need not be stored in either column version.
        assertThat(pick(table, files, true)).isEmpty();
    }

    private FileStoreTable createTable(
            Schema.Builder builder, String clusterColumns, String strategy) throws Exception {
        Schema schema =
                builder.option(CoreOptions.BUCKET.key(), "-1")
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.CLUSTERING_INCREMENTAL.key(), "true")
                        .option(CoreOptions.CLUSTERING_COLUMNS.key(), clusterColumns)
                        .option(CoreOptions.CLUSTERING_STRATEGY.key(), strategy)
                        .option(CoreOptions.NUM_LEVELS.key(), String.valueOf(NUM_LEVELS))
                        .build();
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(tempDir.toString());
        SchemaManager schemaManager = new FileSystemSchemaManager(fileIO, path);
        return FileStoreTableFactory.create(fileIO, path, schemaManager.createTable(schema));
    }

    private static FileStoreTable evolve(FileStoreTable table, SchemaChange... changes)
            throws Exception {
        return table.copy(table.schemaManager().commitChanges(Arrays.asList(changes)));
    }

    private static DataFileMeta file(
            FileStoreTable table,
            String name,
            long firstRowId,
            long sequenceNumber,
            int level,
            @Nullable List<String> writeCols) {
        DataFileMeta file =
                DataFileMeta.forAppend(
                        name + ".parquet",
                        100,
                        3,
                        EMPTY_STATS,
                        sequenceNumber,
                        sequenceNumber,
                        table.schema().id(),
                        Collections.emptyList(),
                        null,
                        FileSource.APPEND,
                        null,
                        null,
                        firstRowId,
                        writeCols);
        return level == 0 ? file : file.upgrade(level);
    }

    private static Optional<CompactUnit> pick(
            FileStoreTable table, List<DataFileMeta> files, boolean full) {
        IncrementalClusterStrategy strategy =
                new IncrementalClusterStrategy(
                        table.schemaManager(), table.coreOptions().clusteringColumns(), 25, 1, 2);
        return strategy.pick(
                NUM_LEVELS,
                new DataEvolutionIncrementalClusterRewriter(table, null)
                        .constructPartitionLevels(files),
                full);
    }

    private static void assertSelectedFiles(
            Optional<CompactUnit> selected, List<DataFileMeta> expected) {
        assertThat(selected).isPresent();
        assertThat(selected.get().outputLevel()).isEqualTo(MAX_LEVEL);
        assertThat(selected.get().files()).containsExactlyInAnyOrderElementsOf(expected);
    }
}
