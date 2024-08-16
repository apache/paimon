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
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SnapshotsTable}. */
public class SnapshotsTableTest extends TableTestBase {
    private static final String tableName = "MyTable";

    private SnapshotsTable snapshotsTable;
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
                        .build();
        snapshotManager = new SnapshotManager(fileIO, tablePath);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(new SchemaManager(fileIO, tablePath), schema);
        FileStoreTable table =
                FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);

        Identifier filesTableId =
                identifier(tableName + Catalog.SYSTEM_TABLE_SPLITTER + SnapshotsTable.SNAPSHOTS);
        snapshotsTable = (SnapshotsTable) catalog.getTable(filesTableId);

        // snapshot 1: append
        write(table, GenericRow.of(1, 1, 1), GenericRow.of(1, 2, 5));

        // snapshot 2: append
        write(table, GenericRow.of(2, 1, 3), GenericRow.of(2, 2, 4));
    }

    @Test
    public void testReadSnapshotsFromLatest() throws Exception {
        List<InternalRow> expectedRow = getExpectedResult(new long[] {1, 2});
        List<InternalRow> result = read(snapshotsTable);
        assertThat(result).containsExactlyInAnyOrderElementsOf(expectedRow);
    }

    private List<InternalRow> getExpectedResult(long[] snapshotIds) {
        List<InternalRow> expectedRow = new ArrayList<>();
        for (long snapshotId : snapshotIds) {
            Snapshot snapshot = snapshotManager.snapshot(snapshotId);
            expectedRow.add(
                    GenericRow.of(
                            snapshotId,
                            snapshot.schemaId(),
                            BinaryString.fromString(snapshot.commitUser()),
                            snapshot.commitIdentifier(),
                            BinaryString.fromString(snapshot.commitKind().toString()),
                            Timestamp.fromLocalDateTime(
                                    LocalDateTime.ofInstant(
                                            Instant.ofEpochMilli(snapshot.timeMillis()),
                                            ZoneId.systemDefault())),
                            BinaryString.fromString(snapshot.baseManifestList()),
                            BinaryString.fromString(snapshot.deltaManifestList()),
                            BinaryString.fromString(snapshot.changelogManifestList()),
                            snapshot.totalRecordCount(),
                            snapshot.deltaRecordCount(),
                            snapshot.changelogRecordCount(),
                            snapshot.watermark()));
        }

        return expectedRow;
    }
}
