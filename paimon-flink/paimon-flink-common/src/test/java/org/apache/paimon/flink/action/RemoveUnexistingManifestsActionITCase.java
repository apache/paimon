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

package org.apache.paimon.flink.action;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.lang.String.format;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.init;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;

/** IT cases for {@link RemoveUnexistingFilesAction}. */
public class RemoveUnexistingManifestsActionITCase extends ActionITCaseBase {

    @Test
    public void testAction() throws Exception {
        init(warehouse);

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), DataTypes.STRING()},
                        new String[] {"k", "v"});
        FileStoreTable table =
                createFileStoreTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        Collections.emptyList(),
                        Collections.singletonMap("manifest.target-file-size", "10 B"));

        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = writeBuilder.newWrite();
        commit = writeBuilder.newCommit();

        List<GenericRow> data = new ArrayList<>();
        data.add(rowData(1L, BinaryString.fromString("Hi")));
        data.add(rowData(2L, BinaryString.fromString("Hello")));
        data.add(rowData(3L, BinaryString.fromString("Paimon")));

        // 3 snapshots
        writeData(data.get(0));
        writeData(data.get(1));
        writeData(data.get(2));

        SnapshotManager snapshotManager = table.snapshotManager();

        List<InternalRow> results = getData(tableName);
        assertThat(results).hasSize(3);
        results.forEach(
                row -> {
                    int pos = (int) row.getLong(0) - 1;
                    assertThat(row.getString(1).toString())
                            .isEqualTo(data.get(pos).getString(1).toString());
                });

        FileStorePathFactory pathFactory = table.store().pathFactory();
        List<ManifestFileMeta> manifestFileMetas =
                table.store()
                        .newScan()
                        .manifestsReader()
                        .read(snapshotManager.latestSnapshot(), ScanMode.ALL)
                        .allManifests;
        Path path = pathFactory.toManifestFilePath(manifestFileMetas.get(1).fileName());
        table.fileIO().delete(path, false);

        assertThatCode(() -> getData(tableName)).hasMessageContaining("not found");

        executeSQL(format("CALL sys.remove_unexisting_manifests('%s.%s')", database, tableName));

        results = getData(tableName);
        assertThat(results).hasSize(2);
        results.forEach(
                row -> {
                    int pos = (int) row.getLong(0) - 1;
                    assertThat(row.getString(1).toString())
                            .isEqualTo(data.get(pos).getString(1).toString());
                });
    }

    @Test
    public void testActionForBranch() throws Exception {
        init(warehouse);

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), DataTypes.STRING()},
                        new String[] {"k", "v"});
        FileStoreTable table =
                createFileStoreTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        Collections.emptyList(),
                        Collections.singletonMap("manifest.target-file-size", "10 B"));

        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = writeBuilder.newWrite();
        commit = writeBuilder.newCommit();

        List<GenericRow> data = new ArrayList<>();
        data.add(rowData(1L, BinaryString.fromString("Hi")));
        data.add(rowData(2L, BinaryString.fromString("Hello")));
        data.add(rowData(3L, BinaryString.fromString("Paimon")));

        // 3 snapshots
        writeData(data.get(0));
        writeData(data.get(1));
        writeData(data.get(2));

        table.createBranch("rt");

        table =
                (FileStoreTable)
                        catalog.getTable(
                                Identifier.fromString(
                                        format("%s.%s$branch_rt", database, tableName)));
        writeBuilder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = writeBuilder.newWrite();
        commit = writeBuilder.newCommit();
        String tableNameBranch = tableName + "$branch_rt";
        List<GenericRow> branchData = new ArrayList<>();
        branchData.add(rowData(4L, BinaryString.fromString("Hi 4")));
        branchData.add(rowData(5L, BinaryString.fromString("Hello 5")));
        branchData.add(rowData(6L, BinaryString.fromString("Paimon 6")));
        // 3 snapshots also
        writeData(branchData.get(0));
        writeData(branchData.get(1));
        writeData(branchData.get(2));

        SnapshotManager snapshotManager = table.snapshotManager();

        List<InternalRow> results = getData(tableNameBranch);
        assertThat(results).hasSize(3);
        results.forEach(
                row -> {
                    int pos = (int) row.getLong(0) - 4;
                    assertThat(row.getString(1).toString())
                            .isEqualTo(branchData.get(pos).getString(1).toString());
                });

        FileStorePathFactory pathFactory = table.store().pathFactory();
        List<ManifestFileMeta> manifestFileMetas =
                table.store()
                        .newScan()
                        .manifestsReader()
                        .read(snapshotManager.latestSnapshot(), ScanMode.ALL)
                        .allManifests;
        Path path = pathFactory.toManifestFilePath(manifestFileMetas.get(1).fileName());
        table.fileIO().delete(path, false);

        assertThatCode(() -> getData(tableNameBranch)).hasMessageContaining("not found");

        executeSQL(
                format("CALL sys.remove_unexisting_manifests('%s.%s')", database, tableNameBranch));

        results = getData(tableNameBranch);
        assertThat(results).hasSize(2);
        results.forEach(
                row -> {
                    int pos = (int) row.getLong(0) - 4;
                    assertThat(row.getString(1).toString())
                            .isEqualTo(branchData.get(pos).getString(1).toString());
                });

        // do not affect main branch
        results = getData(tableName);
        assertThat(results).hasSize(3);
        results.forEach(
                row -> {
                    int pos = (int) row.getLong(0) - 1;
                    assertThat(row.getString(1).toString())
                            .isEqualTo(data.get(pos).getString(1).toString());
                });
    }
}
