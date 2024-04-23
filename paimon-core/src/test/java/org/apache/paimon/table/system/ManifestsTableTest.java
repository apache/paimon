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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.utils.FileStorePathFactoryTest.createNonPartFactory;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link ManifestsTable}. */
public class ManifestsTableTest extends TableTestBase {

    private Table table;
    private ManifestsTable manifestsTable;
    private SnapshotManager snapshotManager;
    private ManifestList manifestList;

    @BeforeEach
    public void before() throws Exception {
        Identifier identifier = identifier("T");
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("pt", DataTypes.INT())
                        .column("col1", DataTypes.INT())
                        .partitionKeys("pt")
                        .primaryKey("pk", "pt")
                        .option(CoreOptions.CHANGELOG_PRODUCER.key(), "input")
                        .option("bucket", "1")
                        .build();
        catalog.createTable(identifier, schema, true);
        table = catalog.getTable(identifier);
        manifestsTable = (ManifestsTable) catalog.getTable(identifier("T$manifests"));

        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(String.format("%s/%s.db/%s", warehouse, database, "T"));
        snapshotManager = new SnapshotManager(fileIO, tablePath);

        ManifestList.Factory factory =
                new ManifestList.Factory(
                        fileIO,
                        FileFormat.fromIdentifier(
                                CoreOptions.MANIFEST_FORMAT.defaultValue().toString(),
                                new Options()),
                        createNonPartFactory(tablePath),
                        null);
        manifestList = factory.create();

        // snapshot 1: append
        write(table, GenericRow.of(1, 1, 1), GenericRow.of(1, 2, 1));

        // snapshot 2: append
        write(table, GenericRow.of(2, 1, 1), GenericRow.of(2, 2, 1));
    }

    @Test
    public void testReadManifestsFromLatest() throws Exception {
        List<InternalRow> expectedRow = getExceptedResult(2L);
        List<InternalRow> result = read(manifestsTable);
        assertThat(result).containsExactlyElementsOf(expectedRow);
    }

    @Test
    public void testReadManifestsFromSpecifiedSnapshot() throws Exception {
        List<InternalRow> expectedRow = getExceptedResult(1L);
        manifestsTable =
                (ManifestsTable)
                        manifestsTable.copy(
                                Collections.singletonMap(CoreOptions.SCAN_SNAPSHOT_ID.key(), "1"));
        List<InternalRow> result = read(manifestsTable);
        assertThat(result).containsExactlyElementsOf(expectedRow);
    }

    @Test
    public void testReadManifestsFromNotExistSnapshot() throws Exception {
        manifestsTable =
                (ManifestsTable)
                        manifestsTable.copy(
                                Collections.singletonMap(CoreOptions.SCAN_SNAPSHOT_ID.key(), "3"));
        List<InternalRow> result = read(manifestsTable);
        assertThat(result).isEmpty();
    }

    private List<InternalRow> getExceptedResult(long snapshotId) {
        if (!snapshotManager.snapshotExists(snapshotId)) {
            return Collections.emptyList();
        }

        Snapshot snapshot = snapshotManager.snapshot(snapshotId);
        List<ManifestFileMeta> allManifestMeta = snapshot.allManifests(manifestList);

        List<InternalRow> expectedRow = new ArrayList<>();
        for (ManifestFileMeta manifestFileMeta : allManifestMeta) {
            expectedRow.add(
                    GenericRow.of(
                            BinaryString.fromString(manifestFileMeta.fileName()),
                            manifestFileMeta.fileSize(),
                            manifestFileMeta.numAddedFiles(),
                            manifestFileMeta.numDeletedFiles(),
                            manifestFileMeta.schemaId()));
        }
        return expectedRow;
    }
}
