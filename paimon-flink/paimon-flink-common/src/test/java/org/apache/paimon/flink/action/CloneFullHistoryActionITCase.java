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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.ExternalPathStrategy;
import org.apache.paimon.Snapshot;
import org.apache.paimon.clone.FullHistoryFileCollector;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.paimon.catalog.Identifier.DEFAULT_MAIN_BRANCH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for full-history mode of {@link CloneAction}. */
public class CloneFullHistoryActionITCase extends ActionITCaseBase {

    @Test
    public void testCloneAllHistoryWithExternalDataPath() throws Exception {
        String sourceExternal = fileUri("source-external");
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(), sourceExternal);
        tableOptions.put(
                CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY.key(),
                ExternalPathStrategy.ROUND_ROBIN.toString());
        FileStoreTable source =
                createFileStoreTable(
                        RowType.of(DataTypes.INT()),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonList("f0"),
                        tableOptions);

        writeRows(source, 0, 1);
        source.createTag("tag1", 1);
        source.createBranch("branch1", "tag1");
        writeRows(source.switchToBranch("branch1"), 1, 2);
        writeRows(source, 1, 3);

        String targetRoot = new Path(getTempDirPath("target-table")).toString();
        String targetExternal = fileUri("target-external");
        Map<String, String> sourceCatalogConfig = Collections.singletonMap("warehouse", warehouse);
        Map<String, String> targetCatalogConfig =
                Collections.singletonMap("warehouse", getTempDirPath("target-warehouse"));
        CloneAction action =
                createAction(
                        source,
                        sourceCatalogConfig,
                        targetCatalogConfig,
                        targetRoot,
                        sourceExternal,
                        targetExternal,
                        true);

        action.run();

        LocalFileIO targetFileIO = LocalFileIO.create();
        Path successFile = new Path(targetRoot, "_SUCCESS");
        assertThat(targetFileIO.exists(successFile)).isTrue();
        FileStoreTable target = FileStoreTableFactory.create(targetFileIO, new Path(targetRoot));
        assertThat(target.snapshotManager().safelyGetAllSnapshots())
                .extracting(snapshot -> snapshot.id())
                .containsExactlyInAnyOrder(1L, 2L);
        assertThat(target.tagManager().tagObjects())
                .extracting(tag -> tag.getRight())
                .containsExactly("tag1");
        assertThat(target.branchManager().branches()).containsExactly("branch1");
        assertThat(target.switchToBranch("branch1").snapshotManager().safelyGetAllSnapshots())
                .extracting(snapshot -> snapshot.id())
                .containsExactlyInAnyOrder(1L, 2L);
        assertThat(target.newScan().plan().splits()).isNotEmpty();

        CloneAction rejectNonEmptyTarget =
                createAction(
                        source,
                        sourceCatalogConfig,
                        targetCatalogConfig,
                        targetRoot,
                        sourceExternal,
                        targetExternal,
                        false);
        assertThatThrownBy(rejectNonEmptyTarget::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Target table root already contains files");

        CloneAction completed =
                createAction(
                        source,
                        sourceCatalogConfig,
                        targetCatalogConfig,
                        targetRoot,
                        sourceExternal,
                        targetExternal,
                        true);
        assertThatThrownBy(completed::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("already completed");

        targetFileIO.delete(successFile, false);
        CloneAction resume =
                createAction(
                        source,
                        sourceCatalogConfig,
                        targetCatalogConfig,
                        targetRoot,
                        sourceExternal,
                        targetExternal,
                        true);
        resume.run();
        assertThat(targetFileIO.exists(successFile)).isTrue();
        FileStoreTable resumed = FileStoreTableFactory.create(targetFileIO, new Path(targetRoot));
        assertThat(resumed.snapshotManager().safelyGetAllSnapshots())
                .extracting(snapshot -> snapshot.id())
                .containsExactlyInAnyOrder(1L, 2L);

        targetFileIO.delete(source.location(), true);
        targetFileIO.delete(new Path(sourceExternal), true);
        validateAllTimeTravel(resumed);
    }

    @Test
    public void testCloneEmptyTable() throws Exception {
        FileStoreTable source =
                createFileStoreTable(
                        RowType.of(DataTypes.INT()),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonList("f0"),
                        Collections.emptyMap());
        String targetRoot = new Path(getTempDirPath("empty-target-table")).toString();
        Map<String, String> sourceCatalogConfig = Collections.singletonMap("warehouse", warehouse);
        Map<String, String> targetCatalogConfig =
                Collections.singletonMap("warehouse", getTempDirPath("empty-target-warehouse"));
        CloneAction action =
                new CloneAction(
                        database,
                        tableName,
                        sourceCatalogConfig,
                        "target_db",
                        "target_table",
                        targetCatalogConfig,
                        4,
                        null,
                        null,
                        null,
                        null,
                        "paimon",
                        "full-history",
                        Collections.singletonList(source.location() + "=" + targetRoot),
                        false,
                        false);

        action.run();

        FileStoreTable target =
                FileStoreTableFactory.create(LocalFileIO.create(), new Path(targetRoot));
        assertThat(target.schemaManager().listAllIds()).containsExactly(0L);
        assertThat(target.snapshotManager().latestSnapshot()).isNull();
    }

    @Test
    public void testCloneUsesTableRootForInternalFileWithNestedMapping() throws Exception {
        Map<String, String> tableOptions =
                Collections.singletonMap(CoreOptions.DATA_FILE_PATH_DIRECTORY.key(), "data");
        FileStoreTable source =
                createFileStoreTable(
                        RowType.of(DataTypes.INT()),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonList("f0"),
                        tableOptions);
        writeRows(source, 0, 1);
        Path sourceDataFile =
                new FullHistoryFileCollector(source).collect().dataFiles().iterator().next();
        Path sourceDataRoot = new Path(source.location(), "data");
        String targetRoot = new Path(getTempDirPath("nested-mapping-target-table")).toString();
        Path targetDataRoot = new Path(getTempDirPath("nested-mapping-target-data"));
        Path targetDataFile =
                new Path(
                        targetDataRoot
                                + sourceDataFile
                                        .toString()
                                        .substring(sourceDataRoot.toString().length()));
        Map<String, String> sourceCatalogConfig = Collections.singletonMap("warehouse", warehouse);
        Map<String, String> targetCatalogConfig =
                Collections.singletonMap(
                        "warehouse", getTempDirPath("nested-mapping-target-warehouse"));
        CloneAction action =
                new CloneAction(
                        database,
                        tableName,
                        sourceCatalogConfig,
                        "target_db",
                        "target_table",
                        targetCatalogConfig,
                        4,
                        null,
                        null,
                        null,
                        null,
                        "paimon",
                        "full-history",
                        Arrays.asList(
                                source.location() + "=" + targetRoot,
                                sourceDataRoot + "=" + targetDataRoot),
                        false,
                        false);

        action.run();

        LocalFileIO targetFileIO = LocalFileIO.create();
        assertThat(targetFileIO.exists(new Path(targetRoot, "_SUCCESS"))).isTrue();
        assertThat(targetFileIO.exists(targetDataFile)).isFalse();
        Path expectedDataFile =
                new Path(
                        targetRoot
                                + sourceDataFile
                                        .toString()
                                        .substring(source.location().toString().length()));
        assertThat(targetFileIO.exists(expectedDataFile)).isTrue();
        FileStoreTable target = FileStoreTableFactory.create(targetFileIO, new Path(targetRoot));
        assertThat(target.newScan().plan().splits()).isNotEmpty();
    }

    private CloneAction createAction(
            FileStoreTable source,
            Map<String, String> sourceCatalogConfig,
            Map<String, String> targetCatalogConfig,
            String targetRoot,
            String sourceExternal,
            String targetExternal,
            boolean cloneIfExists) {
        return new CloneAction(
                database,
                tableName,
                sourceCatalogConfig,
                "target_db",
                "target_table",
                targetCatalogConfig,
                4,
                null,
                null,
                null,
                null,
                "paimon",
                "full-history",
                Arrays.asList(
                        source.location() + "=" + targetRoot,
                        sourceExternal + "=" + targetExternal),
                false,
                cloneIfExists);
    }

    private void writeRows(FileStoreTable table, long commitIdentifier, int... ids)
            throws Exception {
        String user = UUID.randomUUID().toString();
        TableWriteImpl<?> tableWrite = table.newWrite(user);
        TableCommitImpl tableCommit = table.newCommit(user);
        try {
            for (int id : ids) {
                tableWrite.write(GenericRow.of(id));
            }
            tableCommit.commit(commitIdentifier, tableWrite.prepareCommit(true, commitIdentifier));
        } finally {
            tableWrite.close();
            tableCommit.close();
        }
    }

    private void validateAllTimeTravel(FileStoreTable table) throws Exception {
        List<String> branches = new ArrayList<>(table.branchManager().branches());
        branches.add(DEFAULT_MAIN_BRANCH);
        for (String branch : branches) {
            FileStoreTable branchTable = table.switchToBranch(branch);
            for (Snapshot snapshot : branchTable.snapshotManager().safelyGetAllSnapshots()) {
                assertThat(
                                branchTable
                                        .copy(
                                                Collections.singletonMap(
                                                        CoreOptions.SCAN_SNAPSHOT_ID.key(),
                                                        String.valueOf(snapshot.id())))
                                        .newScan()
                                        .plan()
                                        .splits())
                        .isNotEmpty();
            }
            for (Pair<Tag, String> tagAndName : branchTable.tagManager().tagObjects()) {
                assertThat(
                                branchTable
                                        .copy(
                                                Collections.singletonMap(
                                                        CoreOptions.SCAN_TAG_NAME.key(),
                                                        tagAndName.getRight()))
                                        .newScan()
                                        .plan()
                                        .splits())
                        .isNotEmpty();
            }
        }
    }

    private String fileUri(String name) {
        return java.nio.file.Paths.get(getTempDirPath(name)).toUri().toString();
    }
}
