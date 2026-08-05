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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.clone.FullHistoryClonePlan;
import org.apache.paimon.clone.FullHistoryClonePlanner;
import org.apache.paimon.clone.FullHistoryFileCollector;
import org.apache.paimon.clone.FullHistoryFileSet;
import org.apache.paimon.clone.PathMapping;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.DeletionVectorsIndexFile;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
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
import java.util.stream.Collectors;

import static org.apache.paimon.catalog.Identifier.DEFAULT_MAIN_BRANCH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for full-history mode of {@link CloneAction}. */
public class CloneFullHistoryActionITCase extends ActionITCaseBase {

    @Test
    public void testCloneComprehensiveHistoryAndPayloadLayout() throws Exception {
        String sourceExternal1 = fileUri("comprehensive-source-data-1");
        String sourceExternal2 = fileUri("comprehensive-source-data-2");
        String sourceIndex = fileUri("comprehensive-source-index");
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(
                CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(),
                sourceExternal1 + "," + sourceExternal2);
        tableOptions.put(
                CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY.key(),
                ExternalPathStrategy.ROUND_ROBIN.toString());
        tableOptions.put(CoreOptions.GLOBAL_INDEX_EXTERNAL_PATH.key(), sourceIndex);
        tableOptions.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        tableOptions.put(CoreOptions.CHANGELOG_PRODUCER.key(), "lookup");
        tableOptions.put(CoreOptions.LOOKUP_REMOTE_FILE_ENABLED.key(), "true");
        tableOptions.put(CoreOptions.PK_BTREE_INDEX_COLUMNS.key(), "score");
        tableOptions.put(CoreOptions.PK_BITMAP_INDEX_COLUMNS.key(), "category");
        tableOptions.put("file-index.bloom-filter.columns", "note");
        tableOptions.put(CoreOptions.FILE_INDEX_IN_MANIFEST_THRESHOLD.key(), "1 B");
        tableOptions.put(CoreOptions.TARGET_FILE_SIZE.key(), "16 KB");
        tableOptions.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "3");
        tableOptions.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "3");
        tableOptions.put(CoreOptions.CHANGELOG_NUM_RETAINED_MIN.key(), "8");
        tableOptions.put(CoreOptions.CHANGELOG_NUM_RETAINED_MAX.key(), "20");
        FileStoreTable source =
                createFileStoreTable(
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(),
                                    DataTypes.INT(),
                                    DataTypes.STRING(),
                                    DataTypes.STRING()
                                },
                                new String[] {"id", "score", "category", "note"}),
                        Collections.emptyList(),
                        Collections.singletonList("id"),
                        Collections.singletonList("id"),
                        tableOptions);

        try (IOManager ioManager = IOManager.create(getTempDirPath("comprehensive-io"))) {
            List<InternalRow> initialRows = new ArrayList<>();
            for (int id = 1; id <= 24; id++) {
                initialRows.add(row(id, id * 10, "c" + id % 3, largeValue(id)));
            }
            writeRows(source, ioManager, 0, false, initialRows.toArray(new InternalRow[0]));
            source.createTag("v1", 1);
            source.createBranch("audit", "v1");

            FileStoreTable branch = source.switchToBranch("audit");
            writeRows(
                    branch,
                    ioManager,
                    1,
                    true,
                    row(1, 111, "branch", "branch-update"),
                    row(100, 1000, "branch", "branch-insert"));

            writeRows(source, ioManager, 1, true, row(25, 250, "c1", largeValue(25)));
            writeRows(
                    source,
                    ioManager,
                    2,
                    false,
                    row(1, 999, "updated", "main-update"),
                    deleteRow(2, 20, "c2", largeValue(2)),
                    row(26, 260, "c2", "main-insert"));

            catalog.alterTable(
                    Identifier.create(database, tableName),
                    SchemaChange.addColumn("version", DataTypes.STRING()),
                    false);
            source = getFileStoreTable(tableName);
            writeRows(
                    source,
                    ioManager,
                    3,
                    false,
                    evolvedRow(3, 333, "evolved", "schema-v2", "v2"),
                    evolvedRow(27, 270, "evolved", "schema-v2", "v2"));
            writeRows(
                    source,
                    ioManager,
                    4,
                    false,
                    evolvedRow(4, 444, "evolved", "latest", "v2"),
                    evolvedRow(28, 280, "evolved", "latest", "v2"));
        }

        Snapshot analyzedSnapshot = source.snapshotManager().latestSnapshot();
        try (TableCommitImpl tableCommit = source.newCommit(UUID.randomUUID().toString())) {
            tableCommit.updateStatistics(
                    new Statistics(analyzedSnapshot.id(), analyzedSnapshot.schemaId(), 27L, 1024L));
        }
        source.newExpireSnapshots().config(source.coreOptions().expireConfig()).expire();
        source = getFileStoreTable(tableName);
        source.createTag("latest", source.snapshotManager().latestSnapshot().id());

        assertComprehensiveSource(source, sourceExternal1, sourceExternal2, sourceIndex);
        Map<String, List<Integer>> expectedRows = readAllTimeTravel(source);

        String targetRoot = new Path(getTempDirPath("comprehensive-target-table")).toString();
        String targetExternal1 = fileUri("comprehensive-target-data-1");
        String targetExternal2 = fileUri("comprehensive-target-data-2");
        String targetIndex = fileUri("comprehensive-target-index");
        Map<String, String> sourceCatalogConfig = Collections.singletonMap("warehouse", warehouse);
        Map<String, String> targetCatalogConfig =
                Collections.singletonMap(
                        "warehouse", getTempDirPath("comprehensive-target-warehouse"));
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
                                sourceExternal1 + "=" + targetExternal1,
                                sourceExternal2 + "=" + targetExternal2,
                                sourceIndex + "=" + targetIndex),
                        false,
                        false);

        action.run();

        LocalFileIO targetFileIO = LocalFileIO.create();
        FileStoreTable target = FileStoreTableFactory.create(targetFileIO, new Path(targetRoot));
        assertThat(targetFileIO.exists(new Path(targetRoot, "_SUCCESS"))).isTrue();
        assertThat(target.schemaManager().listAllIds())
                .containsExactlyElementsOf(source.schemaManager().listAllIds());
        assertThat(target.snapshotManager().safelyGetAllSnapshots())
                .extracting(Snapshot::id)
                .containsExactlyInAnyOrderElementsOf(
                        source.snapshotManager().safelyGetAllSnapshots().stream()
                                .map(Snapshot::id)
                                .collect(Collectors.toList()));
        assertThat(target.changelogManager().safelyGetAllChangelogs())
                .extracting(changelog -> changelog.id())
                .containsExactlyInAnyOrderElementsOf(
                        source.changelogManager().safelyGetAllChangelogs().stream()
                                .map(changelog -> changelog.id())
                                .collect(Collectors.toList()));
        assertThat(target.tagManager().tagObjects())
                .extracting(Pair::getRight)
                .containsExactlyInAnyOrder("v1", "latest");
        assertThat(target.branchManager().branches()).containsExactly("audit");
        assertRewrittenPayloadRoots(
                target,
                targetExternal1,
                targetExternal2,
                targetIndex,
                source.location().toString(),
                sourceExternal1,
                sourceExternal2,
                sourceIndex);

        targetFileIO.delete(source.location(), true);
        targetFileIO.delete(new Path(sourceExternal1), true);
        targetFileIO.delete(new Path(sourceExternal2), true);
        targetFileIO.delete(new Path(sourceIndex), true);

        target = FileStoreTableFactory.create(targetFileIO, new Path(targetRoot));
        assertThat(readAllTimeTravel(target)).isEqualTo(expectedRows);
        ReadBuilder indexedRead =
                target.newReadBuilder()
                        .withFilter(
                                new org.apache.paimon.predicate.PredicateBuilder(target.rowType())
                                        .equal(1, 999));
        assertThat(readIds(indexedRead)).containsExactly(1);
    }

    private void assertComprehensiveSource(
            FileStoreTable source,
            String sourceExternal1,
            String sourceExternal2,
            String sourceIndex)
            throws Exception {
        assertThat(source.schemaManager().listAllIds()).containsExactly(0L, 1L);
        assertThat(source.snapshotManager().safelyGetAllSnapshots()).hasSize(3);
        assertThat(source.tagManager().tagObjects())
                .extracting(Pair::getRight)
                .containsExactlyInAnyOrder("v1", "latest");
        assertThat(source.branchManager().branches()).containsExactly("audit");
        assertThat(source.changelogManager().safelyGetAllChangelogs()).isNotEmpty();
        assertThat(source.snapshotManager().latestSnapshot().statistics()).isNotNull();
        ManifestList manifestList = source.store().manifestListFactory().create();
        assertThat(source.changelogManager().safelyGetAllChangelogs())
                .anyMatch(
                        changelog ->
                                changelog.changelogManifestList() == null
                                        && (!manifestList.exists(changelog.baseManifestList())
                                                || !manifestList.exists(
                                                        changelog.deltaManifestList())));

        FullHistoryFileSet files = new FullHistoryFileCollector(source).collect();
        assertThat(files.metadataFiles())
                .anyMatch(path -> path.toString().contains("/schema/"))
                .anyMatch(path -> path.toString().contains("/snapshot/"))
                .anyMatch(path -> path.toString().contains("/tag/"))
                .anyMatch(path -> path.toString().contains("/branch/"))
                .anyMatch(path -> path.toString().contains("/changelog/"))
                .anyMatch(path -> path.toString().contains("/statistics/"))
                .anyMatch(path -> path.toString().contains("/manifest/"));
        assertThat(files.dataFiles())
                .anyMatch(path -> path.getName().endsWith(".index"))
                .anyMatch(path -> path.getName().endsWith(".lookup"))
                .anyMatch(path -> isUnder(path, sourceExternal1))
                .anyMatch(path -> isUnder(path, sourceExternal2));
        assertThat(files.indexFiles()).isNotEmpty().allMatch(path -> isUnder(path, sourceIndex));

        List<IndexFileMeta> indexFiles = liveIndexFiles(source);
        assertThat(indexFiles)
                .extracting(IndexFileMeta::indexType)
                .contains(DeletionVectorsIndexFile.DELETION_VECTORS_INDEX, "btree", "bitmap");
        assertThat(indexFiles)
                .allMatch(
                        file ->
                                file.externalPath() != null
                                        && isUnder(new Path(file.externalPath()), sourceIndex));
    }

    private void assertRewrittenPayloadRoots(
            FileStoreTable target,
            String targetExternal1,
            String targetExternal2,
            String targetIndex,
            String... sourceRoots)
            throws Exception {
        FullHistoryFileSet files = new FullHistoryFileCollector(target).collect();
        assertThat(files.dataFiles())
                .anyMatch(path -> path.getName().endsWith(".index"))
                .anyMatch(path -> path.getName().endsWith(".lookup"))
                .anyMatch(path -> isUnder(path, targetExternal1))
                .anyMatch(path -> isUnder(path, targetExternal2));
        assertThat(files.indexFiles()).isNotEmpty().allMatch(path -> isUnder(path, targetIndex));
        assertThat(liveIndexFiles(target))
                .allMatch(
                        file ->
                                file.externalPath() != null
                                        && isUnder(new Path(file.externalPath()), targetIndex));
        for (Path path : files.allFiles()) {
            assertThat(target.fileIO().exists(path)).as("reachable file %s", path).isTrue();
            for (String sourceRoot : sourceRoots) {
                assertThat(isUnder(path, sourceRoot)).as("rewritten path %s", path).isFalse();
            }
        }
    }

    private List<IndexFileMeta> liveIndexFiles(FileStoreTable table) {
        return table.store().newIndexFileHandler().scanEntries().stream()
                .filter(entry -> entry.kind() == FileKind.ADD)
                .map(IndexManifestEntry::indexFile)
                .collect(Collectors.toList());
    }

    private boolean isUnder(Path path, String root) {
        String normalizedPath = path.toString();
        String normalizedRoot = new Path(root).toString();
        return normalizedPath.equals(normalizedRoot)
                || normalizedPath.startsWith(normalizedRoot + "/");
    }

    private GenericRow row(int id, int score, String category, String note) {
        return GenericRow.of(
                id, score, BinaryString.fromString(category), BinaryString.fromString(note));
    }

    private GenericRow deleteRow(int id, int score, String category, String note) {
        return GenericRow.ofKind(
                RowKind.DELETE,
                id,
                score,
                BinaryString.fromString(category),
                BinaryString.fromString(note));
    }

    private GenericRow evolvedRow(int id, int score, String category, String note, String version) {
        return GenericRow.of(
                id,
                score,
                BinaryString.fromString(category),
                BinaryString.fromString(note),
                BinaryString.fromString(version));
    }

    private String largeValue(int seed) {
        StringBuilder builder = new StringBuilder(4096);
        long value = seed;
        for (int i = 0; i < 4096; i++) {
            value = value * 1103515245L + 12345L;
            builder.append((char) ('!' + Math.floorMod(value, 90)));
        }
        return builder.toString();
    }

    private void writeRows(
            FileStoreTable table,
            IOManager ioManager,
            long commitIdentifier,
            boolean compact,
            InternalRow... rows)
            throws Exception {
        String user = UUID.randomUUID().toString();
        TableWriteImpl<?> tableWrite = table.newWrite(user);
        TableCommitImpl tableCommit = table.newCommit(user);
        try {
            tableWrite.withIOManager(ioManager);
            for (InternalRow row : rows) {
                tableWrite.write(row);
            }
            if (compact) {
                tableWrite.compact(BinaryRow.EMPTY_ROW, 0, true);
            }
            tableCommit.commit(commitIdentifier, tableWrite.prepareCommit(true, commitIdentifier));
        } finally {
            tableWrite.close();
            tableCommit.close();
        }
    }

    private List<Integer> readIds(ReadBuilder readBuilder) throws Exception {
        List<Integer> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            reader.forEachRemaining(row -> result.add(row.getInt(0)));
        }
        Collections.sort(result);
        return result;
    }

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
