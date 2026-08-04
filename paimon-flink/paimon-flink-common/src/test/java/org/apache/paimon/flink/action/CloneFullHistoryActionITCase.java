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
import org.apache.paimon.clone.FullHistoryClonePlan;
import org.apache.paimon.clone.FullHistoryClonePlanner;
import org.apache.paimon.clone.FullHistoryFileCollector;
import org.apache.paimon.clone.PathMapping;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.reader.RecordReader;
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

        Map<String, List<Integer>> expectedRows = readAllTimeTravel(source);
        targetFileIO.delete(source.location(), true);
        targetFileIO.delete(new Path(sourceExternal), true);
        assertThat(readAllTimeTravel(resumed)).isEqualTo(expectedRows);
    }

    @Test
    public void testInitialCloneRejectsPopulatedExternalTarget() throws Exception {
        String sourceExternal = fileUri("dirty-source-external");
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

        Path sourceDataFile =
                new FullHistoryFileCollector(source).collect().dataFiles().iterator().next();
        String targetRoot = new Path(getTempDirPath("dirty-target-table")).toString();
        String targetExternal = fileUri("dirty-target-external");
        PathMapping mapping =
                PathMapping.parse(
                        Arrays.asList(
                                source.location() + "=" + targetRoot,
                                sourceExternal + "=" + targetExternal));
        Path targetDataFile = new Path(mapping.rewriteRequired(sourceDataFile.toString()));
        FullHistoryClonePlan plan = new FullHistoryClonePlanner(source, mapping).planStructure();
        assertThat(plan.externalTargetRoots()).containsExactly(new Path(targetExternal));
        LocalFileIO targetFileIO = LocalFileIO.create();
        long sourceSize = source.fileIO().getFileSize(sourceDataFile);
        assertThat(sourceSize).isLessThan(Integer.MAX_VALUE);
        try (PositionOutputStream output = targetFileIO.newOutputStream(targetDataFile, false)) {
            output.write(new byte[(int) sourceSize]);
        }

        Map<String, String> sourceCatalogConfig = Collections.singletonMap("warehouse", warehouse);
        Map<String, String> targetCatalogConfig =
                Collections.singletonMap("warehouse", getTempDirPath("dirty-target-warehouse"));
        CloneAction action =
                createAction(
                        source,
                        sourceCatalogConfig,
                        targetCatalogConfig,
                        targetRoot,
                        sourceExternal,
                        targetExternal,
                        false);

        assertThatThrownBy(action::run)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("external target root")
                .hasMessageContaining(new Path(targetExternal).toString());
        assertThat(targetFileIO.exists(new Path(targetRoot, "_SUCCESS"))).isFalse();
        assertThat(targetFileIO.getFileSize(targetDataFile)).isEqualTo(sourceSize);
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
    public void testCloneAppendOnlyBlobAfterSourceRemoval() throws Exception {
        byte[] blobBytes = "blob-content".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CoreOptions.BUCKET.key(), "-1");
        tableOptions.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        tableOptions.put(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        tableOptions.put(CoreOptions.BLOB_FIELD.key(), "f1");
        FileStoreTable source =
                createFileStoreTable(
                        RowType.of(DataTypes.INT(), DataTypes.BLOB()),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        tableOptions);
        writeRows(source, 0, GenericRow.of(7, new BlobData(blobBytes)));

        String targetRoot = new Path(getTempDirPath("blob-target-table")).toString();
        Map<String, String> sourceCatalogConfig = Collections.singletonMap("warehouse", warehouse);
        Map<String, String> targetCatalogConfig =
                Collections.singletonMap("warehouse", getTempDirPath("blob-target-warehouse"));
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

        LocalFileIO targetFileIO = LocalFileIO.create();
        targetFileIO.delete(source.location(), true);
        FileStoreTable target = FileStoreTableFactory.create(targetFileIO, new Path(targetRoot));
        List<Integer> ids = new ArrayList<>();
        List<byte[]> blobs = new ArrayList<>();
        RecordReader<InternalRow> reader = target.newRead().createReader(target.newScan().plan());
        reader.forEachRemaining(
                row -> {
                    ids.add(row.getInt(0));
                    blobs.add(row.getBlob(1).toData());
                });
        assertThat(ids).containsExactly(7);
        assertThat(blobs).hasSize(1);
        assertThat(blobs.get(0)).isEqualTo(blobBytes);
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
        List<InternalRow> rows = new ArrayList<>();
        for (int id : ids) {
            rows.add(GenericRow.of(id));
        }
        writeRows(table, commitIdentifier, rows.toArray(new InternalRow[0]));
    }

    private void writeRows(FileStoreTable table, long commitIdentifier, InternalRow... rows)
            throws Exception {
        String user = UUID.randomUUID().toString();
        TableWriteImpl<?> tableWrite = table.newWrite(user);
        TableCommitImpl tableCommit = table.newCommit(user);
        try {
            for (InternalRow row : rows) {
                tableWrite.write(row);
            }
            tableCommit.commit(commitIdentifier, tableWrite.prepareCommit(true, commitIdentifier));
        } finally {
            tableWrite.close();
            tableCommit.close();
        }
    }

    private Map<String, List<Integer>> readAllTimeTravel(FileStoreTable table) throws Exception {
        Map<String, List<Integer>> result = new HashMap<>();
        List<String> branches = new ArrayList<>(table.branchManager().branches());
        branches.add(DEFAULT_MAIN_BRANCH);
        for (String branch : branches) {
            FileStoreTable branchTable = table.switchToBranch(branch);
            for (Snapshot snapshot : branchTable.snapshotManager().safelyGetAllSnapshots()) {
                result.put(
                        branch + ":snapshot:" + snapshot.id(),
                        readIds(
                                branchTable.copy(
                                        Collections.singletonMap(
                                                CoreOptions.SCAN_SNAPSHOT_ID.key(),
                                                String.valueOf(snapshot.id())))));
            }
            for (Pair<Tag, String> tagAndName : branchTable.tagManager().tagObjects()) {
                result.put(
                        branch + ":tag:" + tagAndName.getRight(),
                        readIds(
                                branchTable.copy(
                                        Collections.singletonMap(
                                                CoreOptions.SCAN_TAG_NAME.key(),
                                                tagAndName.getRight()))));
            }
        }
        return result;
    }

    private List<Integer> readIds(FileStoreTable table) throws Exception {
        List<Integer> result = new ArrayList<>();
        RecordReader<InternalRow> reader = table.newRead().createReader(table.newScan().plan());
        reader.forEachRemaining(row -> result.add(row.getInt(0)));
        Collections.sort(result);
        return result;
    }

    private String fileUri(String name) {
        return java.nio.file.Paths.get(getTempDirPath(name)).toUri().toString();
    }
}
