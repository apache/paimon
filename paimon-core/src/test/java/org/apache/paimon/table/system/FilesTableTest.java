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

package org.apache.paimon.table.system;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FilesTable}. */
public class FilesTableTest extends TableTestBase {

    private FileStoreTable table;
    private FileStoreScan scan;
    private FilesTable filesTable;
    private SnapshotManager snapshotManager;

    @BeforeEach
    public void before() throws Exception {
        String tableName = "MyTable";
        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(String.format("%s/%s.db/%s", warehouse, database, tableName));
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("pt1", DataTypes.INT())
                        .column("pt2", DataTypes.INT())
                        .column("col1", DataTypes.INT())
                        .partitionKeys("pt1", "pt2")
                        .primaryKey("pk", "pt1", "pt2")
                        .option(CoreOptions.CHANGELOG_PRODUCER.key(), "input")
                        .option(CoreOptions.BUCKET.key(), "2")
                        .option(CoreOptions.SEQUENCE_FIELD.key(), "col1")
                        .build();
        TableSchema tableSchema =
                SchemaUtils.forceCommit(new SchemaManager(fileIO, tablePath), schema);
        table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);
        scan = table.store().newScan();

        Identifier filesTableId =
                identifier(tableName + Catalog.SYSTEM_TABLE_SPLITTER + FilesTable.FILES);
        filesTable = (FilesTable) catalog.getTable(filesTableId);
        snapshotManager = new SnapshotManager(fileIO, tablePath);

        // snapshot 1: append
        write(table, GenericRow.of(1, 1, 10, 1), GenericRow.of(1, 2, 20, 5));

        // snapshot 2: append
        write(table, GenericRow.of(2, 1, 10, 3), GenericRow.of(2, 2, 20, 4));
    }

    @Test
    public void testReadWithFilter() throws Exception {
        compact(table, row(2, 20), 0);
        write(table, GenericRow.of(3, 1, 10, 1));
        assertThat(readPartBucketLevel(null))
                .containsExactlyInAnyOrder(
                        "[1, 10]-0-0", "[1, 10]-0-0", "[1, 10]-1-0", "[2, 20]-0-5");

        PredicateBuilder builder = new PredicateBuilder(FilesTable.TABLE_TYPE);
        assertThat(readPartBucketLevel(builder.equal(0, "[2, 20]")))
                .containsExactlyInAnyOrder("[2, 20]-0-5");
        assertThat(readPartBucketLevel(builder.equal(1, 1)))
                .containsExactlyInAnyOrder("[1, 10]-1-0");
        assertThat(readPartBucketLevel(builder.equal(5, 5)))
                .containsExactlyInAnyOrder("[2, 20]-0-5");
    }

    private List<String> readPartBucketLevel(Predicate predicate) throws IOException {
        ReadBuilder readBuilder = filesTable.newReadBuilder().withFilter(predicate);
        List<String> rows = new ArrayList<>();
        readBuilder
                .newRead()
                .createReader(readBuilder.newScan().plan())
                .forEachRemaining(
                        row ->
                                rows.add(
                                        row.getString(0)
                                                + "-"
                                                + row.getInt(1)
                                                + "-"
                                                + row.getInt(5)));
        return rows;
    }

    @Test
    public void testReadFilesFromLatest() throws Exception {
        List<InternalRow> expectedRow = getExceptedResult(2L);
        List<InternalRow> result = read(filesTable);
        assertThat(result).containsExactlyInAnyOrderElementsOf(expectedRow);
    }

    @Test
    public void testReadWithNotFullPartitionKey() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(FilesTable.TABLE_TYPE);
        assertThat(readPartBucketLevel(builder.equal(0, "[2]"))).isEmpty();
    }

    @Test
    public void testReadFilesFromSpecifiedSnapshot() throws Exception {
        List<InternalRow> expectedRow = getExceptedResult(1L);
        filesTable =
                (FilesTable)
                        filesTable.copy(
                                Collections.singletonMap(CoreOptions.SCAN_SNAPSHOT_ID.key(), "1"));
        List<InternalRow> result = read(filesTable);
        assertThat(result).containsExactlyInAnyOrderElementsOf(expectedRow);
    }

    @Test
    public void testReadFilesFromNotExistSnapshot() throws Exception {

        filesTable =
                (FilesTable)
                        filesTable.copy(
                                Collections.singletonMap(CoreOptions.SCAN_SNAPSHOT_ID.key(), "3"));
        assertThatThrownBy(() -> read(filesTable))
                .satisfies(anyCauseMatches(IllegalArgumentException.class));
    }

    private List<InternalRow> getExceptedResult(long snapshotId) {
        if (!snapshotManager.snapshotExists(snapshotId)) {
            return Collections.emptyList();
        }

        FileStoreScan.Plan plan = scan.withSnapshot(snapshotId).plan();

        List<ManifestEntry> files = plan.files(FileKind.ADD);

        List<InternalRow> expectedRow = new ArrayList<>();
        for (ManifestEntry fileEntry : files) {
            String partition1 = String.valueOf(fileEntry.partition().getInt(0));
            String partition2 = String.valueOf(fileEntry.partition().getInt(1));
            DataFileMeta file = fileEntry.file();
            String minKey = String.valueOf(file.minKey().getInt(0));
            String maxKey = String.valueOf(file.maxKey().getInt(0));
            String minCol1 = String.valueOf(file.valueStats().minValues().getInt(3));
            String maxCol1 = String.valueOf(file.valueStats().maxValues().getInt(3));
            expectedRow.add(
                    GenericRow.of(
                            BinaryString.fromString(
                                    Arrays.toString(new String[] {partition1, partition2})),
                            fileEntry.bucket(),
                            BinaryString.fromString(file.fileName()),
                            BinaryString.fromString(file.fileFormat()),
                            file.schemaId(),
                            file.level(),
                            file.rowCount(),
                            file.fileSize(),
                            BinaryString.fromString(Arrays.toString(new String[] {minKey})),
                            BinaryString.fromString(Arrays.toString(new String[] {maxKey})),
                            BinaryString.fromString(
                                    String.format("{col1=%s, pk=%s, pt1=%s, pt2=%s}", 0, 0, 0, 0)),
                            BinaryString.fromString(
                                    String.format(
                                            "{col1=%s, pk=%s, pt1=%s, pt2=%s}",
                                            minCol1, minKey, partition1, partition2)),
                            BinaryString.fromString(
                                    String.format(
                                            "{col1=%s, pk=%s, pt1=%s, pt2=%s}",
                                            maxCol1, maxKey, partition1, partition2)),
                            file.minSequenceNumber(),
                            file.maxSequenceNumber(),
                            file.creationTime()));
        }
        return expectedRow;
    }
}
