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
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FilesTable}. */
public class FilesTableTest extends TableTestBase {
    private static final String tableName = "MyTable";

    private FileStoreTable table;
    private FileStoreScan scan;
    private FilesTable filesTable;
    private SnapshotManager snapshotManager;

    @BeforeEach
    public void before() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(String.format("%s/%s.db/%s", warehouse, database, tableName));
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("pt", DataTypes.INT())
                        .column("col1", DataTypes.INT())
                        .partitionKeys("pt")
                        .primaryKey("pk", "pt")
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
        write(table, GenericRow.of(1, 1, 1), GenericRow.of(1, 2, 5));

        // snapshot 2: append
        write(table, GenericRow.of(2, 1, 3), GenericRow.of(2, 2, 4));
    }

    @Test
    public void testReadFilesFromLatest() throws Exception {
        List<InternalRow> expectedRow = getExceptedResult(2L);
        List<InternalRow> result = read(filesTable);
        assertThat(result).containsExactlyInAnyOrderElementsOf(expectedRow);
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
    public void testReadFilesFromNotExistSnapshot() {
        filesTable =
                (FilesTable)
                        filesTable.copy(
                                Collections.singletonMap(CoreOptions.SCAN_SNAPSHOT_ID.key(), "3"));
        assertThatThrownBy(() -> read(filesTable))
                .hasRootCauseInstanceOf(FileNotFoundException.class);
    }

    private List<InternalRow> getExceptedResult(long snapshotId) {
        if (!snapshotManager.snapshotExists(snapshotId)) {
            return Collections.emptyList();
        }

        FileStoreScan.Plan plan = scan.withSnapshot(snapshotId).plan();

        List<ManifestEntry> files = plan.files(FileKind.ADD);

        List<InternalRow> expectedRow = new ArrayList<>();
        for (ManifestEntry fileEntry : files) {
            String partition = String.valueOf(fileEntry.partition().getInt(0));
            DataFileMeta file = fileEntry.file();
            String minKey = String.valueOf(file.minKey().getInt(0));
            String maxKey = String.valueOf(file.maxKey().getInt(0));
            String minSequenceNumber = String.valueOf(file.minSequenceNumber());
            String maxSequenceNumber = String.valueOf(file.maxSequenceNumber());
            expectedRow.add(
                    GenericRow.of(
                            BinaryString.fromString(Arrays.toString(new String[] {partition})),
                            fileEntry.bucket(),
                            BinaryString.fromString(file.fileName()),
                            BinaryString.fromString("orc"),
                            file.schemaId(),
                            file.level(),
                            file.rowCount(),
                            file.fileSize(),
                            BinaryString.fromString(Arrays.toString(new String[] {minKey})),
                            BinaryString.fromString(Arrays.toString(new String[] {maxKey})),
                            BinaryString.fromString(
                                    String.format("{col1=%s, pk=%s, pt=%s}", 0, 0, 0)),
                            BinaryString.fromString(
                                    String.format(
                                            "{col1=%s, pk=%s, pt=%s}",
                                            minSequenceNumber, minKey, partition)),
                            BinaryString.fromString(
                                    String.format(
                                            "{col1=%s, pk=%s, pt=%s}",
                                            maxSequenceNumber, maxKey, partition)),
                            file.minSequenceNumber(),
                            file.maxSequenceNumber(),
                            file.creationTime()));
        }
        return expectedRow;
    }
}
